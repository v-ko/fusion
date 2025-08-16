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
        let state = await storageService.headState(projectId);
        expect(state).toBeDefined();

        // Unload
        await storageService.unloadProject(projectId);
        await expect(storageService.headState(projectId)).rejects.toThrow();

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
        expect(update.newCommits).toBeDefined();
        expect(update.newCommits!.length).toBe(1);
        expect(update.newCommits![0].message).toBe("Test commit");

        const newEntities = Object.values(update.newCommits![0].deltaData!);
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
});
