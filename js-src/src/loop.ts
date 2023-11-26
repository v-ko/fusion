import { Callable } from "./util";


class MainLoop {
    callback_stack: Array<[Callable, number, Array<any>]> = [];

    call_delayed(callback: Callable, delay: number = 0, args: Array<any>) {
        throw new Error("Not implemented");
    }

    process_events() {
        throw new Error("Not implemented");
    }
}


class NoMainLoop extends MainLoop {
    call_delayed(callback: Callable, delay: number = 0, args: Array<any>) {
        this.callback_stack.push([callback, Date.now() + delay, args]);
    }

    process_events() {
        const callback_stack = this.callback_stack;
        this.callback_stack = [];
        for (const [callback, call_time, args] of callback_stack) {
            if (Date.now() >= call_time) {
                callback(...args);
            }
        }

        if (this.callback_stack.length > 0) {
            this.process_events();
        }
    }
}


let _main_loop = new NoMainLoop();


export function set_main_loop(main_loop: MainLoop) {
    _main_loop = main_loop;
}

export function main_loop(): MainLoop {
    return _main_loop;
}
