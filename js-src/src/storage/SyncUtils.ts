import { Commit } from "./Commit";
import { BaseAsyncRepository, InternalRepoUpdate, RepoUpdateData } from "./BaseRepository";
import { ChangeType } from "../Change";
import { CommitGraph } from "./CommitGraph";

/**
 * The sync mechanism involves auto-merging and squishing in the commit graph.
 * Each node(=client/device) has its own branch in the repo. Whenever it adds
 * changes (commits) - that's only on its own branch. When a remote node
 * adds a commit and the local repo pulls - that's a non-conflicting change.
 * The local branch auto-merges (adds the commit to the local branch). When all
 * nodes advance to that commit - the history gets squished. Commit ids are not
 * content based, so that doesn't affect the latest commits.
 * Actually all consecutive commits that don't include a head get squished (in
 * order to reduce memory overhead).
 *
 * Special cases and conflict handling
 * * Branches are stored in an ordered faschion. The first ones in the list are
 * considered senior and the last ones - junior

 * - If two nodes add a commit together: the junior one reverts their commit
 *   and merges the senior branch commit. If there's a conflict between the two
 *   commits (same object, same key changed, etc) - the junior node removes
 *   the conflicting changes. Then it adds its own commit. Now that there's no
 *   conflict - the senior node merges the junior commit.
 */

export async function autoMergeForSync(repo: BaseAsyncRepository, localBranchName: string) {
    /**
     * Merge remote commits into the local branch. Follow the rules outlined
     * in the sync procedure above (SyncUtils.ts)
     */
    console.log("Auto-merging for sync")
    let changesMade = false
    let commitGraph = await repo.getCommitGraph()
    let branches = commitGraph.branches()

    let localBranchCommits = commitGraph.branchCommits(localBranchName)
    let otherBranches = branches.filter(b => b.name !== localBranchName)

    let seniorityMap = new Map<string, number>()
    branches.forEach((b, i) => seniorityMap.set(b.name, i))

    let seniorBranches = branches.sort((a, b) => seniorityMap.get(a.name)! - seniorityMap.get(b.name)!)
    let relevantBranches = seniorBranches.filter(b => b.name !== localBranchName)

    let commitsByBranch = new Map<string, Commit[]>()
    branches.forEach(b => commitsByBranch.set(b.name, commitGraph.branchCommits(b.name)))

    // * There should be a squish beforehand
    // Start from the root and go forward on all senior branches in parallel.
    // While there are relevant (senior) branches:
    let currentPos = 0
    while (relevantBranches.length !== 0) {
        // Drop senior branches that are behind (no commit at this pos)
        relevantBranches = relevantBranches.filter(b => commitsByBranch.get(b.name)!.length > currentPos)

        // Drop any branches that differ from the most senior one (from `relevant`)
        // * We sync only from the most senior one that's ahead. Ignore all else.

        // Get the last commit for each branch
        let currentCommits = relevantBranches.map(b => commitsByBranch.get(b.name)!.at(currentPos)!)

        let dominantCommit = currentCommits[0]
        for (let i = 1; i < currentCommits.length; i++) {
            if (currentCommits[i].id !== dominantCommit.id) {
                relevantBranches.splice(i, 1)
            }
        }

        // If the local commit's id is the same with the dominant - continue
        let localCommit = localBranchCommits.at(currentPos)
        if (localCommit && localCommit.id === dominantCommit.id) {
            currentPos++
            continue
        }

        // (else)
        // Merge the commit from the most senior
        // Get the full commit with a delta
        let responce = await repo.getCommits([dominantCommit.id])
        if (responce.length === 0) {
            throw new Error("Full commit info not found")
        }
        dominantCommit = responce[0]

        // - If we're not past the local branch head - there's been a simultaneous
        //   state alteration - call the merge with revert=commitsAhead
        //   which would first revert the commits, then add the remote one, then
        //   re-add them with removed conflicting changes at the lvl1-keys (and new ids)
        //   * There should be max 2 local commits ahead btw - one squished and one head.

        let commitsAheadCount = localBranchCommits.length - currentPos
        let commitsAhead = localBranchCommits.slice(currentPos, commitsAheadCount)

        // Start the merge

        // Revert commits from the local branch that are ahead
        if (commitsAheadCount > 0) {
            await repo.reset({ relativeToHead: -commitsAheadCount })
        }

        // Add the commit from the senior branch
        commitGraph = await repo.getCommitGraph();
        commitGraph.addCommit(dominantCommit)
        repo.applyRepoUpdate({
            commitGraph: commitGraph.data(),
            newCommits: [dominantCommit.data()]
        })


        // Remove conflicting changes from the local commits. I.e. we accept
        // remove any changes that touch the same entities (on update we only
        // remove first-key-level clashes)
        // On dominant delete - remove all commit deltas for that entity
        // On dominant update or create - remove create and delete commit entity
        // deltas. And leave only non-conflicting update 1lvl-keys
        const dominantDelta = dominantCommit.delta
        const dominantEntityIds = dominantDelta.entityIds()
        for (let localCommit of commitsAhead) {
            if (localCommit.deltaData === undefined) {
                throw new Error("Local commit delta data not found")
            }

            let localDelta = localCommit.delta
            for (let entityId of dominantEntityIds) {
                let localChange = localDelta.change(entityId)
                let dominantChange = dominantDelta.change(entityId)

                // Skip dominant entity deltas where there's no local
                if (localChange === undefined) {
                    continue
                }

                let localChangeType = localChange.type()
                let dominantChangeType = dominantChange!.type()
                // throw new Error("None of this is tested. The above line started giving an error after refactoring to Change from ChangeData")

                // Drop data for entity create/delete operations
                if (dominantChangeType === ChangeType.CREATE || dominantChangeType === ChangeType.DELETE) {
                    localCommit.delta.removeChange(entityId)
                } else if (dominantChangeType === ChangeType.UPDATE) {
                    // On dominant update
                    // Drop local delta if it's a create or delete
                    if (localChangeType === ChangeType.CREATE || localChangeType === ChangeType.DELETE) {
                        localCommit.delta.removeChange(entityId)
                    } else if (localChangeType === ChangeType.UPDATE) {
                        // Drop only keys that are present in the dominant
                        let dominantEntityDelta = dominantDelta.changeData(entityId)! // is in dominantDelta.entityIds
                        for (let key of Object.keys(dominantEntityDelta[0])) {
                            let entId: string;
                            let forwardLC: any; // To avoid TS error
                            let reverseLC: any;
                            [entId, forwardLC, reverseLC] = localChange.data // components of the local entity delta
                            if (forwardLC.hasOwnProperty(key)) {
                                delete forwardLC[key]
                                delete reverseLC[key]
                            }
                        }
                        localCommit.delta.removeChange(entityId)
                        localCommit.delta.addChangeFromData(localChange.data)
                    }
                }
            }
        }

        // Re-create the local commits with the removed conflicting changes
        // (and on top of the dominant commit, repo state respectively)
        if (commitsAhead.length > 0) {
            let refreshedCommits: Commit[] = []
            for (let localCommit of commitsAhead) {
                let commit = await repo.commit(localCommit.delta, localCommit.message)
                refreshedCommits.push(commit)
            }
            commitGraph.setBranch(localBranchName, refreshedCommits.at(-1)!.id)
            let localReaddUpdate: RepoUpdateData = {
                commitGraph: commitGraph.data(),
                newCommits: refreshedCommits.map(c => c.data())
            }
            repo.applyRepoUpdate(localReaddUpdate)
        }

        // Move the branch head
        let headCommitId: string;
        if (commitsAhead.length > 0) {
            headCommitId = commitsAhead.at(-1)!.id
        } else {
            headCommitId = dominantCommit.id
        }
        commitGraph.setBranch(localBranchName, headCommitId)

        // Advance one commit
        currentPos++;
    }
}


export function inferRepoChangesFromGraphUpdate(localGraph: CommitGraph, remoteGraph: CommitGraph, newCommitsFull: Commit[]): InternalRepoUpdate {
    let localSet = new Set(localGraph.commits().map((c) => c.id))
    let remoteSet = new Set(remoteGraph.commits().map((c) => c.id))

    // Infer the removed commits
    let removedCommits = localGraph.commits().filter((c) => !remoteSet.has(c.id))

    // Confirm the compatability of reductive changes
    // TODO

    // Infer missing commits from the commit graph
    let addedCommits = remoteGraph.commits().filter((c) => !localSet.has(c.id))

    // Confirm that all missing commits are supplied
    let missingCommitIdSet = new Set(addedCommits.map((c) => c.id))
    let missingCommitsSupplied = newCommitsFull.every((commit) => missingCommitIdSet.has(commit.id))
    if (!missingCommitsSupplied) {
        throw new Error("Missing commits not supplied")
    }

    // Infer the branch changes
    let localBranches = localGraph.branches()
    let remoteBranches = remoteGraph.branches()
    let localMap = new Map(localBranches.map((b) => [b.name, b]))
    let remoteMap = new Map(remoteBranches.map((b) => [b.name, b]))

    // Infer the removed branches
    let removedBranches = localBranches.filter((b) => !remoteMap.has(b.name))

    // Infer the added branches
    let addedBranches = remoteBranches.filter((b) => !localMap.has(b.name))

    // Infer the updated branches
    let updatedBranches = remoteBranches.filter((b) => {
        let localHead = localGraph.headCommit(b.name)
        let remoteHead = remoteGraph.headCommit(b.name)
        return localHead?.id !== remoteHead?.id
    })

    // Sanity checks?

    return {
        addedCommits: newCommitsFull,
        removedCommits: removedCommits,
        addedBranches: addedBranches,
        updatedBranches: updatedBranches,
        removedBranches: removedBranches
    }
}
