interface Command {
    id: string;
    title: string;
    function: Function;

}


let _commands: Command[] = [];

// decorator to register a command
export function command(title: string) {
    return function (target: any, propertyKey: string, descriptor: PropertyDescriptor) {
        _commands.push({
            id: propertyKey,
            title: title,
            function: descriptor.value
        });
    }
}

export function getCommands() {
    return _commands;
}
