@everywhere begin
    using DaggerWebDash
    using Dagger
    using MetaGraphs
    using Graphs

    function notify_graph_finalization(notifications::RemoteChannel, graph_id::Int, final_vertices_promises)
        # println("Entered notify $graph_id")
        for promise in final_vertices_promises
            wait(promise) # Actually, all the promises should be fulfilled at the moment of calling this function
        end
        put!(notifications, graph_id)
    end

    function mock_func()
        sleep(1)
        return
    end
end

# Algorithms
function get_transform(graph::MetaDiGraph, vertex_id::Int)
    type = get_prop(graph, vertex_id, :type)

    # for now just maintain task dependencies at data objects
    if type == "DataObject"
        return (data...) -> data[1]
    end

    f = AVAILABLE_TRANSFORMS[type](graph, vertex_id)

    return (data...) -> Dagger.@spawn f(data...)
end

function _algorithm(graph::MetaDiGraph, vertex_id::Int)
    runtime = get_prop(graph, vertex_id, :runtime_average_s)

    function algorithm(data...)
        println("Gaudi algorithm for vertex $vertex_id !")

        sleep(runtime * 1e4)

        return "$vertex_id"
    end

    return algorithm
end

AVAILABLE_TRANSFORMS = Dict{String, Function}(
    "Algorithm" => _algorithm,
)

function parse_graphs(graphs_map::Dict, output_graph_path::String, output_graph_image_path::String)
    graphs = []
    for (graph_name, graph_path) in graphs_map
        parsed_graph_dot = timestamp_string("$output_graph_path$graph_name") * ".dot"
        parsed_graph_image = timestamp_string("$output_graph_image_path$graph_name") * ".png"
        G = parse_graphml([graph_path])
        
        open(parsed_graph_dot, "w") do f
            MetaGraphs.savedot(f, G)
        end
        dot_to_png(parsed_graph_dot, parsed_graph_image)
        push!(graphs, G)
    end
    return graphs
end

function show_graph(G)
    for (_, v) in enumerate(Graphs.vertices(G))
        println("Node: ")
        print("Node type: ")
        println(get_prop(G, v, :type))
        print("Node class (only for algorithms): ")
        println(get_prop(G, v, :class))
        print("Original name: ")
        println(get_prop(G, v, :original_id))
        print("Node name: ")
        println(get_prop(G, v, :node_id))
        println()
    end
end

# Function to get the map of incoming edges to a vertex (i.e. the sources of the incoming edges)
function get_ine_map(G)
    incoming_edges_sources_map = Dict{eltype(G), Vector{eltype(G)}}()

    for edge in Graphs.edges(G)
        src_vertex = src(edge)
        dest_vertex = dst(edge)
        
        if haskey(incoming_edges_sources_map, dest_vertex)
            push!(incoming_edges_sources_map[dest_vertex], src_vertex)
        else
            incoming_edges_sources_map[dest_vertex] = [src_vertex]
        end
    end

    return incoming_edges_sources_map
end

# Function to get the map of outgoing edges from a vertex (i.e. the destinations of the outgoing edges)
function get_oute_map(G)
    outgoing_edges_destinations_map = Dict{eltype(G), Vector{eltype(G)}}()

    for edge in Graphs.edges(G)
        src_vertex = src(edge)
        dest_vertex = dst(edge)

        if haskey(outgoing_edges_destinations_map, src_vertex)
            push!(outgoing_edges_destinations_map[src_vertex], dest_vertex)
        else
            outgoing_edges_destinations_map[src_vertex] = [dest_vertex]
        end
    end

    return outgoing_edges_destinations_map
end

function get_vertices_promises(vertices::Vector, G::MetaDiGraph)
    promises = []
    for vertex in vertices
        push!(promises, get_prop(G, vertex, :res_data))
    end
    return promises
end

function get_deps_promises(vertex_id, map, G)
    incoming_data = []
    if haskey(map, vertex_id)
        for src in map[vertex_id]
            push!(incoming_data, get_prop(G, src, :res_data))
        end
    end
    return incoming_data
end

function schedule_graph(G::MetaDiGraph)
    inc_e_src_map = get_ine_map(G)

    for vertex_id in MetaGraphs.topological_sort(G) 
        incoming_data = get_deps_promises(vertex_id, inc_e_src_map, G)
        transform = get_transform(G, vertex_id)
        result = transform(incoming_data...)

        set_prop!(G, vertex_id, :res_data, result)
    end
end

function schedule_graph_with_notify(G::MetaDiGraph, notifications::RemoteChannel, graph_id::Int)
    schedule_graph(G)

    final_vertices = []
    out_e_src_map = get_oute_map(G)

    for vertex_id in MetaGraphs.vertices(G)
        if !haskey(out_e_src_map, vertex_id)
            out_e_src_map[vertex_id] = []
        end
    end

    for vertex_id in keys(out_e_src_map)
        if isempty(out_e_src_map[vertex_id])
            push!(final_vertices, vertex_id)
        end
    end

    Dagger.@spawn notify_graph_finalization(notifications, graph_id, get_vertices_promises(final_vertices, G))
end

function flush_logs_to_file()
    ctx = Dagger.Sch.eager_context()
    logs = Dagger.TimespanLogging.get_logs!(ctx)
    open(ctx.log_file, "w") do io
        Dagger.show_plan(io, logs) # Writes graph to a file
    end
end
