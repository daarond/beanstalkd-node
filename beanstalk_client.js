/**
 * Created by daarond on 9/9/2015.
 */

var _ = require("underscore");
var BeanCommandModule = require("./bean_command");

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

    self.send = function(data)
    {
        socket.write(data+"\r\n");
    };

    self.quit = function()
    {
        socket.end();
    };

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

    self.watchTube = function(tube_name)
    {
        var tube = processor.getTube(tube_name);
        if (tube == null){
            self.send('BAD_FORMAT');
        } else {
            self.watching.push(tube_name);
            self.send("WATCHING "+self.watching.length);
        }
    };

    self.ignoreTube = function(tube_name)
    {
        var new_list = _.reject(self.watching, function(_tube){ return _tube.name == tube_name; });

        if (new_list.length == 0){
            self.send('NOT_IGNORED');
        } else {
            self.send("WATCHING "+self.watching.length);
        }
    };

    self.listTubeWatched = function()
    {
        // faking some yaml here
        var msg = "";

        _.forEach(self.watching, function(_tube){
            msg += "- "+_tube+"\r\n";
        });

        msg = "OK "+msg.length+"\r\n" + msg;
        self.send(msg);
        processor.eventCounts[CMD_LIST_TUBES_WATCHED]++;
    };

    self.listTubeUsed = function()
    {
        processor.eventCounts[CMD_LIST_TUBE_USED]++;
        self.send("USING "+self.tube);
    };

    self.createBeanCommand = function()
    {
        // list-tubes-watched, list-tube-used, watch, ignore
        if (/^put /.test(self.command)) {
            if (/^put \d+ \d+ \d+ \d+\r\n$/i.test(subject)) {
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
        } else if (/^quit\r\n$/.test(self.command)) {
            self.socket.end();
        } else if (/^use /i.test(self.command)) {
            var myregexp = /^use (\S+)\r\n$/i;
            var match = myregexp.exec(subject);
            if (match != null) {
                self.useTube(tube);
                processor.eventCounts[CMD_USE]++;
            } else {
                self.send('BAD_FORMAT');
            }
        } else if (/^watch /.test(self.command)) {
            var myregexp = /^watch (\S+)\r\n$/i;
            var match = myregexp.exec(subject);
            if (match != null) {
                self.watchTube(tube);
                processor.eventCounts[CMD_WATCH]++;
            } else {
                self.send('BAD_FORMAT');
            }
        } else if (/^ignore /.test(self.command)) {
            var myregexp = /^ignore (\S+)\r\n$/i;
            var match = myregexp.exec(subject);
            if (match != null) {
                self.ignoreTube(tube);
                processor.eventCounts[CMD_IGNORE]++;
            } else {
                self.send('BAD_FORMAT');
            }
        } else if (/^list-tube-used\r\n$/.test(self.command)) {
            self.listTubeUsed()
        } else if (/^list-tubes-watched\r\n$/.test(self.command)) {
            self.listTubeWatched();
        } else if (/^reserve\r\n$/.test(self.command)
            || /^reserve-with-timeout \d+\r\n$/.test(self.command)
            || /^delete \d+\r\n$/.test(self.command)
            || /^release \d+ \d+ \d+\r\n$/.test(self.command)
            || /^bury \d+ \d+\r\n$/.test(self.command)
            || /^touch \d+\r\n$/.test(self.command)
            || /^peek \d+\r\n$/.test(self.command)
            || /^peek-ready\r\n$/.test(self.command)
            || /^peek-delayed\r\n$/.test(self.command)
            || /^peek-buried\r\n$/.test(self.command)
            || /^kick \d+\r\n$/.test(self.command)
            || /^kick-job \d+\r\n$/.test(self.command)
            || /^stats-job \d+\r\n$/.test(self.command)
            || /^stats-tube \S+\r\n$/.test(self.command)
            || /^stats\r\n$/.test(self.command)
            || /^list-tubes\r\n$/.test(self.command)
            || /^pause-tube \S+ \d+\r\n$/.test(self.command)
        ) {
            var cmd = new BeanCommandModule.BeanCommand();
            cmd.command_type = 0;
            cmd.commandline = self.command.split(' ');
            cmd.client = self;
            cmd.tube = self.tube;
            processor.addToCommandQueue(cmd);
        } else {
            self.send('UNKNOWN_COMMAND');
        }

        // init after add
        self.in_command_state = true;
        self.command = '';
        self.data = '';
    };

    self.dataReceived = function(data)
    {
        data = String(data); // this is a really terrible idea... fix it
        var data_parts = String(data).split(/\r\n/);
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
            } else if (/^put /.test(self.command)){
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
