module TwitterLinks

using Elly
using HadoopBlocks
using Blocks
using Blocks.MatOp

debug = false

##
# Reader
# iteratively read records off a block
# in this case we are readig the whole block at one go as a matrix
read_as_csv(r::HadoopBlocks.HdfsBlockReader, iter_status) = HadoopBlocks.find_rec(r, iter_status, Matrix, '\n', ',')
read_as_tsv(r::HadoopBlocks.HdfsBlockReader, iter_status) = HadoopBlocks.find_rec(r, iter_status, Matrix, '\n', '\t')

##
# Map
# convert list of edges to a sparse matrix
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

##
# Collect
# Merge all edges read on each node
# We have two different type of collect methods
# - one that returns the sparse matrix itself
# - another that returns a remote reference to it
function collectblock(collected, blk)
    HadoopBlocks.logmsg("collect starting...")
    isempty(blk) && (return collected)
    collected = (collected == nothing) ? blk : (collected .+ blk)
    HadoopBlocks.logmsg("collect finished.")
    collected
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

##
# Reduce
# Either merge all parts
# Or collect the references
function reduceblocks(reduced, collected...)
    HadoopBlocks.logmsg("reduce starting...")
    for blk in collected
        reduced = (reduced == nothing) ? blk : (reduced .+ blk)
    end
    HadoopBlocks.logmsg("reduce finished.")
    reduced
end

function reducerefs(reduced, collected...)
    HadoopBlocks.logmsg("reduce starting...")
    for blk in collected
        (reduced == nothing) && (reduced = RemoteRef[])
        isa(blk, RemoteRef) && push!(reduced, blk)
    end
    HadoopBlocks.logmsg("reduce finished.")
    reduced
end

##
# Read all edges as a sparse matrix
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

##
# Calculate pagerank
function _mul(A::DSparseMatrix, b)
    mb = MatOpBlock(A, b, :*, nworkers())
    blk = Block(mb)
    op(blk)
end

function _mul(A::SparseMatrixCSC, b)
    P = S * E
end

function poweriter(A::DSparseMatrix, N=10, delta=1e-10)
    M = size(A, 1)
    b = rand(M, 1)
    last_b = b
    nrm = 0.0

    for iteration in 1:N
        tmp = _mul(A, b)
        nrm = norm(tmp)
        b = tmp ./ nrm
        maxdiff = maximum(abs(b .- last_b))
        if debug
            println("iter $iteration: maxdiff: $maxdiff")
        end
        
        (maxdiff < delta) && break
        last_b = b
    end
    return nrm, b
end

##
# Utility methods
function to_id(v::AbstractString)
    v = strip(v)
    isempty(v) ? 0 : parse(Int32, v)
end
to_id(v::Number) = Int32(v)

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

_eigs(S::DSparseMatrix) = poweriter(S)[2]

function _eigs(S::SparseMatrixCSC)
    println("eigs...")
    eigs(S; nev=1)
    convert(Array{Float64}, E[2])
end

function find_top_influencer(A, E=_eigs(A))
    println("probabilities...")
    P = _mul(A, E)
    P = abs(P[:,1])
    println("getting top...")
    findmax(P)[2]
    #sp = sortperm(P)
    #sp[end-9:end]
end

_count_connections(S::SparseMatrixCSC, id) = (nnz(S[id,:]), nnz(S[:,id]))
function count_connections(S::SparseMatrixCSC, ids)
    for id in ids
        inconn, outconn = _count_connections(S)
        println("id:$id connections: $inconn in $outconn out")
    end
end

function count_connections(S::DSparseMatrix, ids)
    for id in ids
        inconn, outconn = reduce(.+, [[remotecall_fetch((r, id)->_count_connections(fetch(r), id), r.where, r, id)...] for r in S.refs])
        println("id:$id connections: $inconn in $outconn out")
    end
end

function colsum{Tv,Ti}(S::SparseMatrixCSC{Tv,Ti})
    L = size(S, 2)
    colptr = S.colptr
    nzval = S.nzval
    cs = Array(Tv, L)
    for i in 1:L
        csi = zero(Tv)
        for ni in colptr[i]:(colptr[i+1]-1)
            csi += nzval[ni]
        end
        cs[i] = csi
    end
    cs
end
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
    if debug
        println("calculating colsums...")
    end
    cs = colsum(DS)
    println("scheduling normalize...")
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

end # module
