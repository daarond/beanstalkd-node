/**
 * Created by daarond on 9/9/2015.
 */

var _ = require("underscore");
var BeanCommandModule = require("./beanstalk_command");
var BeanProcessorModule = require("./beanstalk_processor");
var yaml = require('js-yaml');

var BeanClient = function(socket, processor)
{
    var self = this;
    self.processor = processor;
    self.socket = socket;
    self.in_command_state = true;
    self.command = '';
    self.data = '';
    self.tube = '';
    self.watching = [];
    self.reserving = false;
    self.reserving_until = 0; // unix time
    self.current_job = null;
    self.isProducer = false;
    self.isWorker = false;

    /**
     * sends data to a socket
     * @param data string to send
     */
    self.send = function(data)
    {
        socket.write(data+"\r\n");
    };

    /**
     * closes a socket
     */
    self.quit = function()
    {
        socket.end();
    };

    /**
     * sets a client to use a specific tube for future commands
     * @param tube_name
     */
    self.useTube = function(tube_name)
    {
        var tube = processor.getTube(tube_name);
        if (tube == null){
            self.send('BAD_FORMAT');
        } else {
            self.tube = tube_name;
            self.send('USING '+tube_name);
        }
    };

    /**
     * adds a tube to the watch list for a client
     * @param tube_name
     */
    self.watchTube = function(tube_name)
    {
        var tube = processor.getTube(tube_name);
        if (tube == null){
            self.send('BAD_FORMAT');
        } else {
            if (_.indexOf(self.watching, tube_name) == -1) {
                // we are not currently watching this tube
                self.watching.push(tube_name);
            }
            self.send("WATCHING "+self.watching.length);
        }
    };

    /**
     * removes a tube from the watch list
     * @param tube_name
     */
    self.ignoreTube = function(tube_name)
    {
        var new_list = _.reject(self.watching, function(_tube){ return _tube == tube_name; });

        if (new_list.length == 0){
            self.send('NOT_IGNORED');
        } else {
            self.send("WATCHING "+self.watching.length);
        }
    };

    /**
     * lists tubes currently watched
     */
    self.listTubeWatched = function()
    {
        var msg = msg = yaml.safeDump(self.watching);

        msg = "OK "+msg.length+"\r\n" + msg;
        self.send(msg);
        processor.eventCounts[BeanProcessorModule.CMD_LIST_TUBES_WATCHED]++;
    };

    /**
     * returns the current tube
     */
    self.listTubeUsed = function()
    {
        processor.eventCounts[BeanProcessorModule.CMD_LIST_TUBE_USED]++;
        self.send("USING "+self.tube);
    };

    /**
     * creates a BeanCommand object for use by the processor
     */
    self.createBeanCommand = function()
    {
        // list-tubes-watched, list-tube-used, watch, ignore
        if (/^put /.test(self.command)) {
            if (/^put \d+ \d+ \d+ \d+$/i.test(self.command)) {
                var cmd = new BeanCommandModule.BeanCommand();
                cmd.command_type = 0;
                cmd.commandline = self.command.split(' ');
                cmd.data = self.data;
                cmd.client = self;
                cmd.tube = self.tube;
                processor.addToCommandQueue(cmd);
                self.isProducer = true;
            } else {
                self.send('BAD_FORMAT');
            }
        } else if (/^quit$/.test(self.command)) {
            self.socket.end();
        } else if (/^use /i.test(self.command)) {
            var myregexp = /^use (\S+)$/i;
            var match = myregexp.exec(self.command);
            if (match != null) {
                self.useTube(match[1]);
                processor.eventCounts[BeanProcessorModule.CMD_USE]++;
            } else {
                self.send('BAD_FORMAT');
            }
        } else if (/^watch /.test(self.command)) {
            var myregexp = /^watch (\S+)$/i;
            var match = myregexp.exec(self.command);
            if (match != null) {
                self.watchTube(match[1]);
                processor.eventCounts[BeanProcessorModule.CMD_WATCH]++;
            } else {
                self.send('BAD_FORMAT');
            }
        } else if (/^ignore /.test(self.command)) {
            var myregexp = /^ignore (\S+)$/i;
            var match = myregexp.exec(self.command);
            if (match != null) {
                self.ignoreTube(match[1]);
                processor.eventCounts[BeanProcessorModule.CMD_IGNORE]++;
            } else {
                self.send('BAD_FORMAT');
            }
        } else if (/^list-tube-used$/.test(self.command)) {
            self.listTubeUsed()
        } else if (/^list-tubes-watched$/.test(self.command)) {
            self.listTubeWatched();
        } else if (/^reserve$/.test(self.command)
            || /^reserve-with-timeout \d+$/.test(self.command)
            || /^delete \d+$/.test(self.command)
            || /^release \d+ \d+ \d+$/.test(self.command)
            || /^bury \d+ \d+$/.test(self.command)
            || /^touch \d+$/.test(self.command)
            || /^peek \d+$/.test(self.command)
            || /^peek-ready$/.test(self.command)
            || /^peek-delayed$/.test(self.command)
            || /^peek-buried$/.test(self.command)
            || /^kick \d+$/.test(self.command)
            || /^kick-job \d+$/.test(self.command)
            || /^stats-job \d+$/.test(self.command)
            || /^stats-tube \S+$/.test(self.command)
            || /^stats$/.test(self.command)
            || /^list-tubes$/.test(self.command)
            || /^pause-tube \S+ \d+$/.test(self.command)
        ) {
            var cmd = new BeanCommandModule.BeanCommand();
            cmd.command_type = 0;
            cmd.commandline = self.command.split(' ');
            cmd.client = self;
            cmd.tube = self.tube;
            processor.addToCommandQueue(cmd);
        } else {
            self.send('BAD_FORMAT');
        }

        // init after add
        self.in_command_state = true;
        self.command = '';
        self.data = '';
    };

    /**
     * handles data received by the socket
     * @param data
     */
    self.dataReceived = function(data)
    {
        data = String(data);
        var data_parts = String(data).split(/\r\n/);
        data_parts = _.compact(data_parts);
        var ends_in_crlf = /\r\n$/.test(data);

        var counter = 0;

        _.forEach(data_parts, function (part) {
            var last_item = counter+1==data_parts.length;
            counter++;

            // add the data part where we need it
            if (self.in_command_state){
                self.command += part;
            } else {
                self.data += part;
            }

            if (self.in_command_state
                && !/^put /.test(self.command)
                && (!last_item   // this is not the last item
                || (last_item && ends_in_crlf)) // last item and string terminates with CRLF
            ){
                // command is not a put and is complete, so process it
                self.createBeanCommand();
            } else if (self.in_command_state
                && /^put /.test(self.command)){
                // it is a put, so we go to data mode
                self.in_command_state = false;
            } else if (!self.in_command_state    // in a data state
                && /^put /.test(self.command) // in a put, where we should be
                && (!last_item   // this is not the last item
                || (last_item && ends_in_crlf)) // last item and string terminates with CRLF
            ) {
                // good to go on put command
                self.createBeanCommand();
            }
        });
    }
};

exports.BeanClient = BeanClient;
