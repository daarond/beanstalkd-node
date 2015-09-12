/**
 * Created by daarond on 9/9/2015.
 */
var moment = require('moment');
var fs = require('fs');
var define = require("node-constants")(exports);


var BeanJob = function(client, tube, priority, time_to_run, delay_seconds)
{
    var self = this;

    constants.define(jobStates, {
        READY:    0,
        DELAYED:  1,
        RESERVED: 2,
        BURIED:   3
    });

    constants.define(counter, {
        RESERVES: 0,
        TIMEOUTS: 1,
        RELEASES: 2,
        BURIES:   3,
        KICKS:    4
    });

    self.data = '';
    self.client = client;
    self.tube = tube;
    self.id = moment().format('x');
    self.eventCounts = [];
    self.state = jobStates.READY;
    self.time_to_run = time_to_run;
    self.delay_until = moment().add(delay_seconds, 'seconds');
    self.timeout_at = moment().add(delay_seconds + time_to_run, 'seconds');
    self.priority = priority;

    self.updateFile = function()
    {
        fs.writeFile("./jobs/"+self.id+".bsjob", JSON.stringify(self), function(err) {
            if(err) {
                return console.log(err);
            }
            console.log("Job file "+self.id+".bsjob updated\n");
        });
    };

    self.delete = function()
    {
        fs.unlink("./jobs/"+self.id+".bsjob", function(err) {
            if(err) {
                return console.log(err);
            }
            console.log("Job file "+self.id+".bsjob deleted\n");
        });
    };

    self.release = function(delay_seconds)
    {
        self.eventCounts[COUNTER_RELEASES]++;

        if (delay_seconds == null || delay_seconds == 0) {
            self.state = jobStates.READY;
        } else {
            self.state = jobStates.DELAYED;
            self.delay_until = moment().add(delay_seconds, 'seconds');
        }

        self.updateFile();
    };

    self.kick = function()
    {
        self.eventCounts[COUNTER_KICKS]++;
        self.state = jobStates.READY;
        self.updateFile();
    };

    self.bury = function()
    {
        self.eventCounts[COUNTER_BURIES]++;
        self.state = jobStates.BURIED;
        self.updateFile();
    };

    self.reserve = function(sender)
    {
        self.eventCounts[COUNTER_RESERVES]++;
        sender.current_job = job;
        self.state = jobStates.RESERVED;

        // send a response
        var response = "RESERVED "+self.id+" "+self.data.length+"\r\n";
        response += self.data;
        sender.send(response);

        self.updateFile();
    };

    self.touch = function()
    {
        self.timeout_at = moment().add(self.time_to_run, 'seconds');
        self.updateFile();
    };

    self.timeout = function()
    {
        self.eventCounts[COUNTER_TIMEOUTS]++;
        self.client.current_job = null;
        self.client = null;
        self.state = jobStates.READY;
        self.updateFile();
    };
};

exports.BeanJob = BeanJob;
