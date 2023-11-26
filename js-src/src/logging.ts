let LOG_LEVEL = 0;  // 0: info, 1: warning, 2: error

class Logger {
    name: string;

    constructor(name: string) {
        this.name = name;
    }
    info(...args: any[]): void {
        if (LOG_LEVEL > 0) {
            return;
        }
        console.info(`[${this.name}]`, ...args);
    }
    warning(...args: any[]): void {
        if (LOG_LEVEL > 1) {
            return;
        }
        console.warn(`[${this.name}]`, ...args);
    }
    error(...args: any[]): void {
        console.error(`[${this.name}]`, ...args);
    }
}

export function getLogger(name: string): Logger {
    return new Logger(name);
}
