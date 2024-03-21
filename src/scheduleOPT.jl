module readDraft

import IterTools

using DataFrames
using Dates
using HiGHS
using JuMP
using Unicode
using XLSX

function sheetdf(sh::XLSX.Worksheet, coalvalue::Union{Int,String}; flip::Bool=false, dates::Bool=false)
    if flip && dates
        return sh |>
               XLSX.eachtablerow |>
               DataFrame |>
               (df) -> coalesce.(df, coalvalue) |>
                       (df) -> permutedims(df, 1) |>
                               (df) -> transform!(df, :date => ByRow((x) -> Date(x)) => :date)
    elseif dates
        return sh |>
               XLSX.eachtablerow |>
               DataFrame |>
               (df) -> coalesce.(df, coalvalue) |>
                       (df) -> transform!(df, :date => ByRow((x) -> Date(x)) => :date)
    else
        return sh |>
               XLSX.eachtablerow |>
               DataFrame |>
               (df) -> coalesce.(df, coalvalue)
    end
end


function validate_names(people::DataFrame, pools::DataFrame, limits::DataFrame, restrictions::DataFrame)
    report = DataFrame(lastname=[], missing_from=[])
    nameslist = limits.lastname

    for name in nameslist
        if !(name in names(restrictions))
            push!(report, (name, "restrictions"))
        end
        if !(name in people.lastname)
            push!(report, (name, "people"))
        end
        flag = true
        for col in eachcol(pools)
            if name in col
                flag = false
            end
        end
        if flag
            push!(report, (name, "pools"))
        end
    end

    return report
end

function validate_needs(pools::DataFrame, shifts::DataFrame, needs::DataFrame, limits::DataFrame)
    required_shifts = names(needs[:, Not(:date)])

    flag = true
    for shift in required_shifts
        # get which shifts combine with shift
        combinelist = shifts[shifts.shift.==shift, :combines_with]
        # get how many of shift do we need
        requiredcount = sum(eachrow(needs[:, shift]))[1]
        nameslist = String[]
        # get names in the pool of shift
        append!(nameslist, pools[:, shift])
        # get names in pools of shifts that combine with shift
        if length(combinelist) > 0
            for comb in combinelist
                if comb != ""
                    append!(nameslist, pools[:, comb])
                end
            end
        end
        # filter "" from names
        filter!((x) -> x != "", nameslist)

        availablecount = combine(limits[in.(limits.lastname, Ref(nameslist)), Not(:lastname)], All() .=> sum .=> All())[1, :limit]

        if requiredcount > availablecount
            println("shift: ", shift, " required: ", requiredcount, " available: ", availablecount)
            flag = false
        end
    end

    if flag
        println("needs OK")
    end

    return flag
end

function validatesheets(restrictions, needs, shifts, pools, people, weights, limits)
    is_valid = true
    # check for name errors
    report = validate_names(people, pools, limits, restrictions)

    if nrow(report) > 0
        show(report)
        is_valid = false
    else
        println("names OK")
    end

    # check dates
    if restrictions.date != needs.date
        println("different dates")
        is_valid = false
    else
        println("dates OK")
    end

    # check shifts
    if shifts.shift != names(pools)
        println("different pools")
        is_valid = false
    else
        println("shifts OK")
        println("pools OK")
    end

    # check if needs can be covered
    if !validate_needs(pools, shifts, needs, limits)
        is_valid = false
    end

    # check restrictions
    # TODO

    if is_valid
        println("all pass OK")
    end

    return is_valid
end

function history_fromdirectory(directory)
    dfs = []
    for file in sort(readdir(directory))
        filename = joinpath(directory, file)
        println(filename)
        wb = XLSX.readxlsx(filename)
        push!(dfs, sheetdf(wb["final"], ""))
        # append!(data, rawdata_fromxlsx(filename))
    end

    history = dfs[1]
    for df in dfs[2:end]
        history = leftjoin(history, df, on=[:date])
    end
    history = permutedims(history, 1) |> (df) -> coalesce.(df, "")
    transform!(history, :date => ByRow((x) -> Date(x)) => :date)

    return history
end

function main(filename::String, directory::String)
    wb = XLSX.readxlsx(filename)

    restrictions = sheetdf(wb["restrictions"], "", flip=true, dates=true)
    needs = sheetdf(wb["needs"], 0)
    shifts = sheetdf(wb["shifts"], "")
    pools = sheetdf(wb["pools"], "")
    people = sheetdf(wb["people"], "")
    transform!(people, :unit .=> ByRow(string) => :unit)
    weights = sheetdf(wb["weights"], "")
    limits = sheetdf(wb["limits"], 0)

    validatesheets(restrictions, needs, shifts, pools, people, weights, limits)
    people = leftjoin(people, limits, on=[:lastname]) |> (x) -> coalesce.(x, "")

    history = history_fromdirectory(directory)

    # values to exclude from counting
    excluded_values = ["Χ", "?", "??", "!", "!!", "Ε", "Α", "ΕΦ", ""]

    # stack all lastnames
    stacked = stack(history, 3:length(names(history)), variable_name=:lastname, value_name=:shift)
    # filter out excluded shifts
    filter!(:shift => x -> !(x in excluded_values), stacked)
    # count shifts per daytype
    gd_counts = combine(groupby(stacked, [:lastname, :daytype]), nrow => :count)
    # count total shift
    gd_total = combine(groupby(stacked, [:lastname]), nrow => :total)
    # create month column
    stacked.month = month.(stacked.date)
    # create year column
    stacked.year = year.(stacked.date)
    # count months per person
    gd_months = combine(groupby(unique(stacked, [:lastname, :month, :year]), :lastname), nrow => :months)
    # join to create cost column equal to total/months
    stacked = transform(leftjoin(gd_counts, gd_months, on=:lastname), [:count, :months] => ByRow((count, months) -> count / months) => :cost)
    # unstack to have lastnames as columns
    costs = unstack(stacked, :daytype, :lastname, :cost, fill=0)
    # show(costs)

    # TODO find a good formula for the weights
    weights.factor = [100 / (x / 1.25) for x in 1:nrow(weights)] .* weights.factor
    # show(weights)
    weights = unstack(weights, :weight, :factor)
    weights[!, ""] = [1]
    # show(weights[:, "Χ"])

    preferences = stack(restrictions, 3:length(names(restrictions)))
    # show(preferences)

    costsnames = names(costs)
    for name in names(restrictions, Not(:date, :daytype))
        # if a name is used for the first time the it will not have costs
        # so we set name's costs to 0
        if !(name in costsnames)
            costs[!, name] .= 0
        end

        # TODO this cannot handle preassigned shifts on the restrictions sheet yet
        transform!(
            restrictions,
            Cols("daytype", name) =>
                # TODO formula for costs
                ByRow((dt, n) -> costs[costs.daytype.==dt, name][1] * weights[:, n][1] + weights[:, n][1]) =>
                    name
        )
    end
    restrictions = stack(restrictions, 3:length(names(restrictions)))
    restrictions.pref = preferences.value

    stacked_pools = stack(pools, 1:8)
    stacked_pools = stacked_pools[stacked_pools.value .!= "", :]
    rename!(stacked_pools, :value => :lastname, :variable => :shift)
    rename!(restrictions, :variable => :lastname)

    # show(restrictions)
    # show(stacked_pools)

    # create vars df
    vars = leftjoin(restrictions, stacked_pools, on=[:lastname])
    transform!(vars, :daytype => ByRow((x) -> occursin("H", x) || occursin("S", x) ||occursin("T", x)) => :hday)
    vars = leftjoin(vars, limits, on=[:lastname])
    needs = stack(needs, 2:length(names(needs)), variable_name=:shift, value_name=:need)
    vars = leftjoin(vars, needs, on=[:date, :shift]) |> (df) -> coalesce.(df, 0)
    vars = leftjoin(vars, shifts, on=[:shift]) |> (df) -> coalesce.(df, "")
    transform!(vars, :combines_with => ByRow((x) -> x in names(pools) ) => :has_combination)
    vars.id = 1:nrow(vars)

    # Create model and variables
    model = Model()
    @variable(model, x[vars.id], Bin)

    # person total limits
    for group in groupby(vars, :lastname)
        @constraint(model, sum(x[group.id]) <= group.limit[1])
        @constraint(model, sum(x[group[group.hday, :id]]) <= group.holiday[1])
    end

    # apply needs
    # TODO check if this really adds the combination shifts
    dateshiftgroups = groupby(vars, [:date, :shift])
    for (key, group) in pairs(dateshiftgroups)
        ids = []
        append!(ids, group.id)
        if group.has_combination[1]
            combination = group.combines_with[1]
            new_key = Dict("date" => key[1], "shift" => combination)
            append!(ids, dateshiftgroups[new_key].id)
        end
        if group.need[1] > 0
            @constraint(model, sum(x[ids]) == group.need[1])
        end
    end

    # at most 1 shift per day
    # obsolete because of the consecutive days part
    # for group in groupby(vars, [:date, :lastname])
    #     @constraint(model, sum(x[group.id]) <= 1)
    # end

    # no consecutive days
    distance_objective = []
    for group in groupby(vars, :lastname)
        for part in IterTools.partition(unique(group.date), 2, 1)
            @constraint(model, sum(x[group[in.(group.date, Ref(part)), :id]]) <= 1)
        end
        for part in IterTools.subsets(collect(enumerate(unique(group.date))), Val(2))
            distance = 1/(part[2][1] - part[1][1])
            dates = [part[1][2], part[2][2]]
            push!(distance_objective, sum(x[group[in.(group.date, Ref(dates)), :id]]/distance))
        end
    end

    # Add objective
    # TODO this formula affects results too
    @objective(model, Min, sum(x[vars.id] .* vars.value)+sum(distance_objective))


    # show(model)
    set_silent(model)
    set_optimizer(model, HiGHS.Optimizer)
    optimize!(model)
    vars.sol = abs.(value.(x))

    solution = combine(groupby(vars, :sol)[Dict(:sol => 1)], [:date, :daytype, :pref, :value, :lastname, :shift])

    println()
    println("HIGH COSTS ------------------------------------------------------------------")
    show(solution[solution.pref .!= "", :])

    
    println()
    println("ASSIGNEMENTS ----------------------------------------------------------------")
    assignements = sort(unstack(solution, [:date, :daytype], :shift, :lastname, fill=""), :date)
    show(assignements)

    println(solution_summary(model))
    println(objective_value(model))
    show(model)

    XLSX.writetable("df.xlsx", assignements)
    return vars
end

println()
vars = readDraft.main("draft_1_24.xlsx", "months")
println()

end
