module TwitterLinks

using Elly
using HadoopBlocks

read_as_csv(r::HadoopBlocks.HdfsBlockReader, iter_status) = HadoopBlocks.find_rec(r, iter_status, Matrix, '\n', ',')
read_as_tsv(r::HadoopBlocks.HdfsBlockReader, iter_status) = HadoopBlocks.find_rec(r, iter_status, Matrix, '\n', '\t')

function to_id(v::AbstractString)
    v = strip(v)
    isempty(v) ? 0 : parse(Int32, v)
end
to_id(v::Number) = Int32(v)

function mapblock(blk::Matrix)
    HadoopBlocks.logmsg("map starting...")
    I = Int32[]
    J = Int32[]
    for idx in 1:size(blk,1)
        i = to_id(blk[idx,2])
        j = to_id(blk[idx,1])
        if (i > 0) && (j > 0)
            push!(I, i)
            push!(J, j)
        end
    end
    L = length(I)
    S = sparse(I, J, ones(L), MAXNODE, MAXNODE)
    HadoopBlocks.logmsg("map finished.")
    rets = SparseMatrixCSC[]
    push!(rets, S)
    rets
end

function collectblock(collected, blk)
    HadoopBlocks.logmsg("collect starting...")
    isempty(blk) && (return collected)
    collected = (collected == nothing) ? blk : (collected .+ blk)
    HadoopBlocks.logmsg("collect finished.")
    collected
end

function reduceblocks(reduced, collected...)
    HadoopBlocks.logmsg("reduce starting...")
    for blk in collected
        reduced = (reduced == nothing) ? blk : (reduced .+ blk)
    end
    HadoopBlocks.logmsg("reduce finished.")
    reduced
end

function wait_results(j_mon)
    while true
        jstatus,jstatusinfo = status(j_mon,true)
        ((jstatus == "error") || (jstatus == "complete")) && break
        (jstatus == "running") && println("$(j_mon): $(jstatusinfo)% complete...")
        sleep(5)
    end
    wait(j_mon)
    println("time taken (total time, wait time, run time): $(times(j_mon))")
    println("")
end

function _as_sparse(file, typ)
    f = (typ === :csv) ? read_as_csv : read_as_tsv
    j = dmapreduce(MRHdfsFileInput([file], f), mapblock, collectblock, reduceblocks)
    wait_results(j)
    R = results(j)
    (R[1] == "complete") && (return R[2])
    error("job in error $(R[2])")
end

function normalize_cols(S)
    println("normalizing...")
    colptr = S.colptr
    rowval = S.rowval
    nzval = S.nzval
    for j = 1:size(S,2)
        j1 = colptr[j]
        j2 = colptr[j+1]
        l = j2-j1
        for jidx in j1:(j2-1)
            nzval[jidx] /= l
        end
    end
end

function _eigs(S)
    println("eigs...")
    eigs(S; nev=1)
end

function find_influencers(S, E=_eigs(S))
    EV = convert(Array{Float64}, E[2])
    println("probabilities...")
    P = S * EV
    P = abs(P[:,1])
    println("getting top...")
    sp = sortperm(P)
    sp[end-9:end]
end

function count_connections(S, ids)
    for id in ids
        println("id:$id connections: $(nnz(S[id,:])) in $(nnz(S[:,id])) out")
    end
end

end # module
