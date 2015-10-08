module TwitterLinks

using Elly
using HadoopBlocks
using Blocks
using Blocks.MatOp

read_as_csv(r::HadoopBlocks.HdfsBlockReader, iter_status) = HadoopBlocks.find_rec(r, iter_status, Matrix, '\n', ',')
read_as_tsv(r::HadoopBlocks.HdfsBlockReader, iter_status) = HadoopBlocks.find_rec(r, iter_status, Matrix, '\n', '\t')

function to_id(v::AbstractString)
    v = strip(v)
    isempty(v) ? 0 : parse(Int32, v)
end
to_id(v::Number) = Int32(v)

function mapblock(blk::Matrix, MAXNODE)
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

function collectrefs(collected, blk)
    HadoopBlocks.logmsg("collect starting...")
    isempty(blk) && (return collected)
    if collected == nothing
        collected_blk = blk
        collected = RemoteRef()
    else
        collected_blk = take!(collected) .+ blk
    end
    put!(collected, collected_blk)
    HadoopBlocks.logmsg("collect finished.")
    collected
end

function reducerefs(reduced, collected...)
    HadoopBlocks.logmsg("reduce starting...")
    for blk in collected
        reduced = (reduced == nothing) ? RemoteRef[] : push!(reduced, blk)
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

function as_sparse(file, typ, dim)
    f = (typ === :csv) ? read_as_csv : read_as_tsv
    j = dmapreduce(MRHdfsFileInput([file], f), (blk)->mapblock(blk, dim), collectblock, reduceblocks)
    wait_results(j)
    R = results(j)
    (R[1] == "complete") && (return R[2])
    error("job in error $(R[2])")
end

function as_distributed_sparse(file, typ, dim)
    f = (typ === :csv) ? read_as_csv : read_as_tsv
    j = dmapreduce(MRHdfsFileInput([file], f), (blk)->mapblock(blk, dim), collectrefs, reducerefs)
    wait_results(j)
    R = results(j)
    (R[1] == "complete") && (return DSparseMatrix(Float64, (dim, dim), R[2]))
    error("job in error $(R[2])")
end

colsum(S::SparseMatrixCSC) = [sum(S[:,x]) for x in 1:size(S,2)]
colsum(DS::DSparseMatrix) = reduce(.+, [remotecall_fetch((r)->colsum(fetch(r)), r.where, r) for r in DS.refs])

function normalize_cols(S::SparseMatrixCSC, fact)
    println("normalizing...")
    colptr = S.colptr
    rowval = S.rowval
    nzval = S.nzval
    for j = 1:size(S,2)
        j1 = colptr[j]
        j2 = colptr[j+1]
        for jidx in j1:(j2-1)
            nzval[jidx] /= fact[j]
        end
    end
end

function normalize_cols(DS::DSparseMatrix)
    cs = colsum(DS)
    for idx in 1:length(DS.refs)
        ref = DS.refs[idx]
        remotecall_wait((r,cs)->normalize_cols(fetch(r), cs), ref.where, ref, cs)
    end
end

function normalize_cols(S::SparseMatrixCSC)
    colptr = S.colptr
    fact = [colptr[j+1]-colptr[j] for j in 1:size(S,2)]
    normalize_cols(S, fact)
end

function _mul(A::DSparseMatrix, b)
    mb = MatOpBlock(A, b, :*, nworkers())
    blk = Block(mb)
    op(blk)
end

function _mul(A::SparseMatrixCSC, b)
    P = S * E
    abs(P[:,1])
end

function poweriter(A::DSparseMatrix, N=100, delta=1e-10)
    M = size(A, 1)
    b = rand(M, 1)
    last_b = copy(b)
    nrm = 0.0

    for iteration in 1:N
        tmp = dsparse_mul(A, b)
        nrm = norm(tmp)
        b = tmp ./ nrm
        maxdiff = maximum(abs(b .- last_b))
        println("iter $iteration: maxdiff: $maxdiff")
        
        (maxdiff < delta) && break
        last_b = copy(b)
    end
    return nrm, b
end

_eigs(S::DSparseMatrix) = poweriter(S)[2]

function _eigs(S::SparseMatrixCSC)
    println("eigs...")
    eigs(S; nev=1)
    convert(Array{Float64}, E[2])
end

function find_influencers(A, E=_eigs(A))
    println("probabilities...")
    P = _mul(A, E)
    println("getting top...")
    sp = sortperm(P)
    sp[end-9:end]
end

function count_connections(S::SparseMatrixCSC, ids)
    for id in ids
        println("id:$id connections: $(nnz(S[id,:])) in $(nnz(S[:,id])) out")
    end
end

end # module
