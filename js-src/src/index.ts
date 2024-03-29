import { ActionState } from "./libs/Action";
import { Channel, addChannel } from "./libs/Channel";
import { getLogger } from "./logging";

const log = getLogger('facade');


class FusionFacade {
    _reproducibleIds: boolean = false;
    rootActionEventsChannel: Channel;

    constructor() {
        // Catch start/end of root actions and push them to the rootActionEventsChannel
        this.rootActionEventsChannel = addChannel('rootActionEvents');

        // // Test action middleware by logging
        // this.rootActionEventsChannel.subscribe((actionState: ActionState) => {
        //     log.info(`Action ${actionState.name} ${actionState.runState}`);
        // });
    }
    get reproducibleIds(): boolean {
        return this._reproducibleIds;
    }
    set reproducibleIds(reproducibleIds: boolean) {
        this._reproducibleIds = reproducibleIds;
    }
}

export const fusion = new FusionFacade();
