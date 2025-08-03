export type Callable = (...args: any[]) => any;

let objectCounter = 0;
let _fakeTime: Date | null = null;  // For testing purposes
let _deterministicIds = false;

// export function get_counted_id(obj) {
//   if (typeof obj.__uniqueid === "undefined") {
//     obj.__uniqueid = ++objectCounter;
//   }
//   return obj.__uniqueid;
// }

const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789';

interface ICryptoModule {
    subtle: SubtleCrypto;
}

let _cryptoModule: ICryptoModule;

if (typeof window !== 'undefined') {
    // Browser main thread environment
    _cryptoModule = window.crypto;
} else if (typeof self !== 'undefined' && (self as any).crypto) {
    // Service worker environment - use self.crypto
    _cryptoModule = (self as any).crypto;
} else {
    // Node.js test environment - use dynamic require for crypto module
    // eslint-disable-next-line
    _cryptoModule = require('crypto').webcrypto;
}

export function cryptoModule(): SubtleCrypto {
    return _cryptoModule.subtle;
}

export function createId(length: number = 8): string {
    const idLength = 8;
    let id = '';

    if (typeof window !== 'undefined' && window.crypto && !_deterministicIds) {
        const randomBytes = new Uint8Array(idLength);
        window.crypto.getRandomValues(randomBytes);

        for (let i = 0; i < idLength; i++) {
            id += alphabet[randomBytes[i] % alphabet.length];
        }
    } else { // in testing or
        for (let i = 0; i < 8; i++) {
            const index = Math.floor(Math.random() * alphabet.length);
            id += alphabet.charAt(index);
        }
    }

    return id;
}

export function currentTime(): Date {
    if (_fakeTime) {
        return _fakeTime;
    }

    return new Date();
}

export function timestamp(dt: Date, microseconds: boolean = false): string {
    if (microseconds) {
        return dt.toISOString();
    } else {
        return dt.toISOString().split('.')[0] + 'Z';
    }
}

/**
 * Generate a filename-appropriate timestamp with microseconds for unique image filenames.
 * Format: YYYY-MM-DD_HH-MM-SS_microseconds
 * Example: 2025-07-04_16-50-30_123456
 */
export function generateFilenameTimestamp(): string {
    const now = new Date();

    // Get date components
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');

    // Get time components
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');

    // Get microseconds (milliseconds * 1000 + random component for uniqueness)
    const milliseconds = now.getMilliseconds();
    const microseconds = String(milliseconds * 1000 + Math.floor(Math.random() * 1000)).padStart(6, '0');

    return `${year}-${month}-${day}_${hours}-${minutes}-${seconds}_${microseconds}`;
}

export function degreesToRadians(degrees: number) {
    return degrees * (Math.PI / 180);
}

// export function isRunningInDesktopApp(): boolean {
//   // Type assertion to inform TypeScript about electronAPI on the window object
//   const desktopWindow = window as any;
//   return desktopWindow.curatorDesktopAPI !== undefined;
// }
