import { ProjectStorageManager, ProjectStorageConfig } from "./ProjectStorageManager";
import { clearInMemoryAdapterInstances, StorageAdapterConfig } from "../repository/Repository";
import { indexConfigs } from "../test-utils";


const INMEM_PROJECT_STORAGE_CONFIG: StorageAdapterConfig = {
    name: 'InMemorySingletonForTesting',
    args: {
        localBranchName: 'dev1',
        projectId: 'test-project-id',
        indexConfig: indexConfigs
    }
}

describe("ProjectStorageManager base functionality", () => {
    let projectStorageManager: ProjectStorageManager;
    let projectId: string;
    let projectStorageConfig: ProjectStorageConfig;

    beforeEach(() => {
        projectId = 'test-project-id';

        projectStorageConfig = {
            deviceBranchName: 'dev1',
            storeIndexConfigs: indexConfigs,
            onDeviceRepo: INMEM_PROJECT_STORAGE_CONFIG,
            onDeviceMediaStore: {
                name: "InMemory",
                args: { projectId }
            }
        };

        projectStorageManager = new ProjectStorageManager(projectStorageConfig);
    });
    afterEach(async () => {
        clearInMemoryAdapterInstances();
    });

    test("Create and load project", async () => {
        // Create project
        await projectStorageManager.createProject();
        expect(projectStorageManager.onDeviceRepo).toBeDefined();
        expect(projectStorageManager.mediaStore).toBeDefined();
        expect(projectStorageManager.onDeviceRepo.hashTree).toBeDefined();
        expect(projectStorageManager.onDeviceRepo._commitGraph).toBeDefined();
        expect(projectStorageManager.onDeviceRepo._commitGraph.branches()).toHaveLength(1);

        // Shutdown and reload
        projectStorageManager.shutdown();
    });

    test("Erase local storage", async () => {
        await projectStorageManager.createProject();

        // Should not throw
        await projectStorageManager.eraseLocalStorage();

        projectStorageManager.shutdown();
    });

    test("Erase local storage before initialization", async () => {
        // Should not throw, just log warning
        await projectStorageManager.eraseLocalStorage();
    });

    test("Constructor throws on branch name mismatch", () => {
        const mismatchedConfig = {
            ...projectStorageConfig,
            deviceBranchName: 'different-branch'
        };

        expect(() => {
            new ProjectStorageManager(mismatchedConfig);
        }).toThrow();
    });
});
