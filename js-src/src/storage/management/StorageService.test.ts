import { Change } from "fusion/model/Change";
import { Delta } from "fusion/model/Delta";
import { ProjectStorageConfig } from "fusion/storage/management/ProjectStorageManager";
import * as StorageServiceModule from "fusion/storage/management/StorageService";
import { clearInMemoryAdapterInstances, RepoUpdateData, StorageAdapterConfig } from "fusion/storage/repository/Repository";
import { indexConfigs, DummyNote, DummyPage } from "fusion/storage/test-utils";
import { createId } from "fusion/util/base";
import { addChannel, Channel } from "fusion/registries/Channel";


const INMEM_PROJECT_STORAGE_CONFIG: StorageAdapterConfig = {
    name: 'InMemorySingletonForTesting',
    args: {
        localBranchName: 'dev1',
        projectId: 'test-project-id',
        indexConfig: indexConfigs
    }
}


// Await the next call after `callsBefore`
function waitForNextCall<T extends jest.Mock>(
    fn: T,
    callsBefore = fn.mock.calls.length,
    timeoutMs = 5000
): Promise<Parameters<T>> {
    return new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error(`Timeout after ${timeoutMs}ms`)), timeoutMs);
        const poll = () => {
            if (fn.mock.calls.length > callsBefore) {
                clearTimeout(t);
                resolve(fn.mock.calls[fn.mock.calls.length - 1] as Parameters<T>);
            } else {
                setTimeout(poll, 0);
            }
        };
        poll();
    });
}



describe("StorageService base functionality", () => {
    let storageService: StorageServiceModule.StorageService;
    let projectId: string;
    let projectStorageConfig: ProjectStorageConfig;

    beforeEach(async () => {
        // To avoid mixing up events between tests, we need to have separate channels for each.
        // So we mock the channel getter to return a unique channel for each test.
        jest.spyOn(StorageServiceModule, 'getStorageUpdatesChannel').mockImplementation((): Channel => {
            const testSpecificName = `${StorageServiceModule.LOCAL_STORAGE_UPDATE_CHANNEL}-${createId()}`;
            const backend = (typeof BroadcastChannel !== 'undefined') ? 'broadcast' : 'local';
            return addChannel(testSpecificName, { backend: backend });
        });

        let bc = new BroadcastChannel(StorageServiceModule.LOCAL_STORAGE_UPDATE_CHANNEL + '2');

        storageService = new StorageServiceModule.StorageService();
        storageService.setupInMainThread();
        projectId = 'test-project-id';

        projectStorageConfig = {
            deviceBranchName: 'dev1',
            storeIndexConfigs: indexConfigs,
            onDeviceStorageAdapter: INMEM_PROJECT_STORAGE_CONFIG,
            onDeviceMediaStore: {
                name: "InMemory",
                args: { projectId }
            }
        };
    });

    afterEach(async () => {
        jest.restoreAllMocks();
        clearInMemoryAdapterInstances();
        storageService.disconnect();
    });


    test("sanity: waitForNextCall resolves for a plain mock invocation", async () => {
        const fn = jest.fn<void, [any]>();

        const p = waitForNextCall(fn);
        // Schedule the call in a microtask to avoid timer shenanigans
        queueMicrotask(() => fn({ ok: true }));

        const [arg] = await p;
        expect(arg).toEqual({ ok: true });
    });

    test("Project load and unload", async () => {
        // Create
        await storageService.createProject(projectId, projectStorageConfig);

        // Load
        await storageService.loadProject(projectId, projectStorageConfig, () => { });
        let commitGraph = await storageService.getCommitGraph(projectId);
        expect(commitGraph).toBeDefined();

        // Unload
        await storageService.unloadProject(projectId);
        await expect(storageService.getCommitGraph(projectId)).rejects.toThrow();

        // Second unload should fail
        await expect(storageService.unloadProject(projectId)).rejects.toThrow();

        // Delete to avoid errors on the next test
        await storageService.deleteProject(projectId, projectStorageConfig);
    });

    test("Project create and delete", async () => {
        // Create
        await storageService.createProject(projectId, projectStorageConfig);

        // Try to create again, expect failure for duplicate id
        await expect(storageService.createProject(projectId, projectStorageConfig)).rejects.toThrow();

        // Delete
        await storageService.deleteProject(projectId, projectStorageConfig);

        // Try to load deleted project
        await expect(storageService.loadProject(projectId, projectStorageConfig, () => { })).rejects.toThrow();
    });

    test("Commit to project", async () => {
        // Create project
        await storageService.createProject(projectId, projectStorageConfig);

        const onUpdate = jest.fn<void, [RepoUpdateData]>();
        await storageService.loadProject(projectId, projectStorageConfig, onUpdate);

        // If loadProject already triggered updates, remember how many
        const callsBefore = onUpdate.mock.calls.length;

        const page = new DummyPage({ id: createId(), parent_id: '', name: "Test Page" });
        const note = new DummyNote({ id: createId(), parent_id: page.id, testProp: "Test Note" });
        const delta = Delta.fromChanges([Change.create(page), Change.create(note)]);

        // commit is fire-and-forget (returns void)
        storageService.commit(projectId, delta.data, "Test commit");

        // Wait for the *next* callback invocation after commit
        const [update] = await waitForNextCall(onUpdate, callsBefore);

        // Assertions based on StorageServiceActual._exectuteCommitRequest()
        expect(update).toBeDefined();
        expect(update.upsertedCommits).toBeDefined();
        expect(update.upsertedCommits!.length).toBe(1);
        expect(update.upsertedCommits![0].message).toBe("Test commit");

        const newEntities = Object.values(update.upsertedCommits![0].deltaData!);
        expect(newEntities.length).toBe(2);
    });

    test('Media operations', async () => {
        // Load project
        await storageService.createProject(projectId, projectStorageConfig);
        await storageService.loadProject(projectId, projectStorageConfig, () => { });

        // Add a media blob
        const blob = new Blob(['test content'], { type: 'text/plain' });
        const mediaData = await storageService.addMedia(projectId, blob, '/test.txt', 'some-parent-id');

        expect(mediaData).toBeDefined();
        expect(mediaData.id).toBeDefined();
        expect(mediaData.contentHash).toBeDefined();

        // Get the media blob
        const retrievedBlob = await storageService.getMedia(projectId, mediaData.id, mediaData.contentHash);
        expect(retrievedBlob).toBeDefined();
        expect(retrievedBlob.size).toBe(blob.size);
        expect(retrievedBlob.type).toBe(blob.type);
        expect(await retrievedBlob.text()).toBe('test content');

        // Remove the media
        await storageService.removeMedia(projectId, mediaData.id, mediaData.contentHash);

        // Verify it's removed by trying to get it again
        await expect(storageService.getMedia(projectId, mediaData.id, mediaData.contentHash)).rejects.toThrow();
    });
    test("Squash old prefix commits by TTL", async () => {
        // Create and load project
        await storageService.createProject(projectId, projectStorageConfig);

        const onUpdate = jest.fn<void, [RepoUpdateData]>();
        await storageService.loadProject(projectId, projectStorageConfig, onUpdate);

        const callsBefore = onUpdate.mock.calls.length;

        // Control time to make the first two commits "old" relative to the third one
        const day = 24 * 60 * 60 * 1000;
        let now = 1_700_000_000_000; // arbitrary fixed base
        const nowSpy = jest.spyOn(Date, "now").mockImplementation(() => now);

        // Commit 0: create a page
        const page = new DummyPage({ id: createId(), parent_id: "", name: "P0" });
        const delta0 = Delta.fromChanges([Change.create(page)]);
        storageService.commit(projectId, delta0.data, "c0");
        const [u0] = await waitForNextCall(onUpdate, callsBefore);

        const commits0 = u0.commitGraph.commits;
        expect(commits0.length).toBe(1);
        const c0Id = commits0[0].id;

        // Commit 1: create a note under the page (still "old")
        now = now + 1; // slightly after c0
        const note1 = new DummyNote({ id: createId(), parent_id: page.id, testProp: "N1" });
        const delta1 = Delta.fromChanges([Change.create(note1)]);
        storageService.commit(projectId, delta1.data, "c1");
        const [u1] = await waitForNextCall(onUpdate, onUpdate.mock.calls.length);

        const commits1 = u1.commitGraph.commits;
        expect(commits1.length).toBe(2);

        // Identify c1 = the child of c0
        const c1Meta = commits1.find(c => c.parentId === c0Id)!;
        expect(c1Meta).toBeDefined();
        const c1Id = c1Meta.id;

        // Commit 2: advance time past the TTL so [c0..c1] are old and get squashed into c0
        now = now + 3 * day; // ensure cutoff >= c1.timestamp
        const note2 = new DummyNote({ id: createId(), parent_id: page.id, testProp: "N2" });
        const delta2 = Delta.fromChanges([Change.create(note2)]);
        storageService.commit(projectId, delta2.data, "c2");
        const [u2] = await waitForNextCall(onUpdate, onUpdate.mock.calls.length);

        const commits2 = u2.commitGraph.commits;

        // After squash (new logic): commits should be [c1(updated with c0's parent and aggregated delta), c2]
        // The new squash logic deletes c0 and updates c1 to have c0's parent and aggregated content
        expect(commits2.length).toBe(2);

        // Debug: let's see what we actually got
        console.log('Commits after c2:', commits2.map(c => ({ id: c.id, parentId: c.parentId, timestamp: c.timestamp })));
        console.log('Expected c0Id:', c0Id, 'c1Id:', c1Id);

        // c0 should be deleted after squash
        const c0AfterSquash = commits2.find(c => c.id === c0Id);
        expect(c0AfterSquash).toBeUndefined();

        // c1 should be updated with c0's parent (empty string) and aggregated delta
        const c1Updated = commits2.find(c => c.id === c1Id)!;
        expect(c1Updated).toBeDefined();
        expect(c1Updated.parentId).toBe(''); // Should now have c0's parent (empty string)

        // c2 should remain unchanged - find it by looking for commit that's not c1
        const c2AfterSquash = commits2.find(c => c.id !== c1Id)!;
        expect(c2AfterSquash).toBeDefined();
        expect(c2AfterSquash.parentId).toBe(c1Id); // Still points to c1
        const c2Id = c2AfterSquash.id;

        // The commit IDs should be c1 and c2
        const commitIds2 = new Set(commits2.map(c => c.id));
        expect(commitIds2.has(c0Id)).toBe(false); // c0 removed
        expect(commitIds2.has(c1Id)).toBe(true);  // c1 kept (updated)
        expect(commitIds2.has(c2Id)).toBe(true);  // c2 kept

        // Head should still point to c2
        const headId = u2.commitGraph.branches.find(b => b.name === "dev1")!.headCommitId!;
        expect(headId).toBe(c2Id);

        // Cleanup of Date.now spy handled by afterEach via jest.restoreAllMocks()
        nowSpy.mockRestore();
    });

});
