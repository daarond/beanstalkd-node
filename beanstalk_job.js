/**
 * Created by daarond on 9/9/2015.
 */
var moment = require('moment');
var fs = require('fs');
var BeanJobModule = require('./beanstalk_job');

exports.JOBSTATE_READY    = 0;
exports.JOBSTATE_DELAYED  = 1;
exports.JOBSTATE_RESERVED = 2;
exports.JOBSTATE_BURIED   = 3;
exports.COUNTER_RESERVES  = 0;
exports.COUNTER_TIMEOUTS  = 1;
exports.COUNTER_RELEASES  = 2;
exports.COUNTER_BURIES    = 3;
exports.COUNTER_KICKS     = 4;


var BeanJob = function(client, tube, priority, time_to_run, delay_seconds)
{
    var self = this;

    self.id = 0;
    self.data = '';
    self.client = client;
    self.tube = tube;
    self.eventCounts = [];
    self.state = BeanJobModule.JOBSTATE_READY;
    self.time_to_run = time_to_run;
    self.delay_until = moment().add(delay_seconds, 'seconds');
    self.timeout_at = moment().add(delay_seconds + time_to_run, 'seconds');
    self.priority = priority;

    self.assignId = function()
    {
        self.id = moment().format('x')+Math.floor(Math.random() * 100);
    };
    self.assignId();

    self.updateFile = function()
    {
        /*
        // for future development...
        fs.writeFile("./jobs/"+self.id+".bsjob", JSON.stringify(self), function(err) {
            if(err) {
                return console.log(err);
            }
            console.log("Job file "+self.id+".bsjob updated\n");
        });
        */
    };

    self.delete = function()
    {
        /*
         // for future development...
        fs.unlink("./jobs/"+self.id+".bsjob", function(err) {
            if(err) {
                return console.log(err);
            }
            console.log("Job file "+self.id+".bsjob deleted\n");
        });
        */
    };

    self.release = function(delay_seconds)
    {
        if (delay_seconds == null || delay_seconds == 0) {
            self.state = BeanJobModule.JOBSTATE_READY;
            self.eventCounts[BeanJobModule.COUNTER_RELEASES]++;
        } else {
            self.state = BeanJobModule.JOBSTATE_DELAYED;
            self.delay_until = moment().add(delay_seconds, 'seconds');
        }

        self.updateFile();
    };

    self.kick = function()
    {
        self.eventCounts[BeanJobModule.COUNTER_KICKS]++;
        self.state = BeanJobModule.JOBSTATE_READY;
        self.updateFile();
    };

    self.bury = function()
    {
        self.eventCounts[BeanJobModule.COUNTER_BURIES]++;
        self.state = BeanJobModule.JOBSTATE_BURIED;
        self.updateFile();
    };

    self.reserve = function(sender)
    {
        self.eventCounts[BeanJobModule.COUNTER_RESERVES]++;
        sender.current_job = self;
        self.state = BeanJobModule.JOBSTATE_RESERVED;

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
        self.eventCounts[BeanJobModule.COUNTER_TIMEOUTS]++;
        self.client.current_job = null;
        self.client = null;
        self.state = BeanJobModule.JOBSTATE_READY;
        self.updateFile();
    };
};

exports.BeanJob = BeanJob;
