/**
 * Created by daarond on 9/9/2015.
 */

var BeanTubeModule = require('./beanstalk_tube');
var BeanJobModule = require('./beanstalk_job');
var BeanClientModule = require('./beanstalk_client');
var moment = require("moment");
var _ = require("underscore");

const CMD_PUT = 0;
const CMD_PEEK = 1;
const CMD_PEEK_READY = 2;
const CMD_PEEK_DELAYED = 3;
const CMD_PEEK_BURIED = 4;
const CMD_RESERVE = 5;
const CMD_USE = 6;
const CMD_WATCH = 7;
const CMD_IGNORE = 8;
const CMD_DELETE = 9;
const CMD_RELEASE = 10;
const CMD_BURY = 11;
const CMD_KICK = 12;
const CMD_STATS = 13;
const CMD_STATS_JOB = 14;
const CMD_STATS_TUBE = 15;
const CMD_LIST_TUBES = 16;
const CMD_LIST_TUBE_USED = 17;
const CMD_LIST_TUBES_WATCHED = 18;
const CMD_PAUSE_TUBE = 19;
const CMD_JOB_TIMEOUT = 20;
const CMD_TOTAL_JOBS = 21;
const CMD_TOTAL_CONNECTIONS = 22;
const CMD_TOUCH = 23;

// https://raw.githubusercontent.com/kr/beanstalkd/master/doc/protocol.txt

var BeanProcessor = function()
{
    var self = this;
    self.id = moment().format('x');
    self.drain_mode = false;
    self.freeze = true;
    self.jobs_list = [];
    self.command_list = [];
    self.tubes = [];
    self.start_time = moment().format('X');
    self.total_jobs = 0;
    self.eventCounts = [];

    self.addToCommandQueue = function(bean_command) {
        self.command_list.push(bean_command);
    };

    self.processCycle = function()
    {
        self.processCommandList();
        self.checkJobTimeout();
        self.checkClientReserves();
        self.deleteUnusedTubes();

        if (!self.freeze){
            self.check_interval_timer = setTimeout(self.processCycle, 3000);
        }
    };

    //self.check_interval_timer = setTimeout(self.processCycle, 3000);

    self.processCommandList = function()
    {
        var list = _.clone(self.command_list);
        _.forEach(list, function (_cmd) {
            self.processCommand(_cmd);
        })
    };

    self.processCommand = function(bean_command)
    {
        var command = bean_command.commandline[0];
        if (command == 'put'){
            self.commandPut(bean_command.client, bean_command.tube, bean_command.commandline[1],
                bean_command.commandline[2], bean_command.commandline[3],
                bean_command.commandline[4], bean_command.data
            );
        } else if (command == 'reserve'){
            self.commandReserve(bean_command.client);
        } else if (command == 'reserve-with-timeout'){
            self.commandReserveTimeout(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'delete'){
            self.commandDelete(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'release') {
            self.commandRelease(bean_command.client, bean_command.commandline[1],
                bean_command.commandline[2], bean_command.commandline[3]);
        } else if (command == 'bury'){
            self.commandBury(bean_command.client, bean_command.commandline[1],
                bean_command.commandline[2]);
        } else if (command == 'touch'){
            self.commandTouch(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'peek'){
            self.commandTouch(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'peek-ready'){
            self.commandPeekState(bean_command.client, BeanJobModule.JOBSTATE_READY);
        } else if (command == 'peek-delayed'){
            self.commandReserve(bean_command.client, BeanJobModule.JOBSTATE_DELAYED);
        } else if (command == 'peek-buried'){
            self.commandReserve(bean_command.client, BeanJobModule.JOBSTATE_BURIED);
        } else if (command == 'kick'){
            self.commandKickTube(bean_command.client, bean_command.commandline[1], bean_command.commandline[2]);
        } else if (command == 'kick-job'){
            self.commandKickJob(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'stats-job'){
            self.commandStatsJob(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'stats-tube'){
            self.commandStatsTube(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'stats'){
            self.commandStats(bean_command.client);
        } else if (command == 'list-tubes'){
            self.commandStats(bean_command.client);
        } else if (command == 'pause-tube'){
            self.commandKickTube(bean_command.client, bean_command.commandline[1], bean_command.commandline[2]);
        }
    };

    self.checkJobTimeout = function()
    {
        _.forEach(self.jobs_list, function (_job) {
            if (_job.state == BeanJobModule.JOBSTATE_RESERVED
                && _job.timeout_at.add(2, 'second').isAfter()){
                // if 1 second before timeout, send DEADLINE_SOON
                _job.client.send('DEADLINE_SOON');
            } else if (_job.state == BeanJobModule.JOBSTATE_RESERVED
                && _job.timeout_at.add(1, 'second').isAfter()){
                // if beyond timeout, send TIMED_OUT
                _job.timeout();
                self.eventCounts[CMD_JOB_TIMEOUT]++;
                _job.client.send('TIMED_OUT');
            } else if (_job.state == BeanJobModule.JOBSTATE_DELAYED
                && _job.timeout_at.add(1, 'second').isAfter()){
                // if we're delayed and ready to go
                _job.release(0);
            }
        });
    };

    self.checkTubesPaused = function()
    {
        _.forEach(self.tubes, function(_tube){
            if (_tube.paused
                && _tube.pause_until.add(1, 'second').isAfter()) {
                _tube.paused = false;
            }
        });
    };

    self.checkClientReserves = function()
    {
        _.forEach(bean_clients, function(_client){
            if (_client.reserving
            && _client.reserving_until.isBefore()) {
                var job = self.getJobsForClient(_client);
                if (job != null){
                    _client.isWorker = true;
                    job.reserve(_client);
                }
            }
        });
    };

    self.deleteUnusedTubes = function()
    {
        // get the list of tubes used by jobs
        var used_list = _.pluck(self.jobs_list, 'tube');

        // get the list of tubes used by clients
        used_list.concat(_.pluck(bean_clients, 'tube'));

        _.uniq(used_list);

        _.forEach(self.tubes, function(_tube){
            if (_.indexOf(used_list, _tube) == null){
                self.tubes = _.reject(self.tubes, function(unused_tube){ return unused_tube.name == _tube });
            }
        });
    };

    self.findJob = function(job_id)
    {
        return _.find(self.jobs_list, function(_job){ return _job.id == job_id; });
    };

    self.findTube = function(tube_name)
    {
        return _.find(self.tubes, function(_tube){ return _tube.name == tube_name; });
    };

    self.getTube = function(tube_name)
    {
        var tube = self.findTube(tube_name);

        // no tube with this name? create one!
        if (tube == null){
            tube = new BeanTubeModule.Tube();
            tube.name = tube_name;
            self.tubes.push(tube);
        }

        return tube;
    };

    self.deleteTube = function(tube_name)
    {
        self.tubes = _.reject(self.tubes, function(_tube){ return _tube.name == tube_name; });
    };

    self.isBadTubeName = function(tube_name)
    {
        var tube = tube_name.trim();
        return /[^a-z0-9-+\/;.$_()]/i.test(tube) || tube.length==0 || tube.length > 200;
    };

    self.commandPut = function(sender, tube, priority, delay, time_to_run, byte_count, data)
    {
        if (self.drain_mode){
            sender.send('DRAINING');
            return;
        }

        if (data.length != parseInt(byte_count)){
            sender.send('EXPECTED_CRLF');
            return;
        }

        if (parseInt(priority) < 0 || parseInt(priority) > 4294967295){
            sender.send('BAD_FORMAT');
            return;
        }

        tube = tube.trim();
        if (self.isBadTubeName(tube)) {
            sender.send('BAD_FORMAT');
            return;
        }
        self.getTube(tube);

        // add it to the jobs list
        var job = new BeanJobModule.BeanJob(sender, tube, priority, time_to_run, delay);
        if (delay > 0){
            job.state = BeanJobModule.JOBSTATE_DELAYED;
        }
        job.data = data;
        job.updateFile();
        self.jobs_list.push(job);

        self.eventCounts[CMD_PUT]++;
        self.eventCounts[CMD_TOTAL_JOBS]++;
        job.total_jobs++;

        sender.send('INSERTED '+job.id);
    };

    self.commandDelete = function(sender, job_id)
    {
        // remove it from the jobs list, unless reserved by someone else
        var newlist = [];
        var found = false;
        _.forEach(self.jobs_list, function(_job){
            if (_job.id == job_id){
                found = true;

                // someone else has this job
                if (_job.client != sender){
                    sender.send("NOT_FOUND");
                    return;
                }

                // clear their current job
                if (sender == _job.client){
                    sender.current_job = null;
                }

                _job.delete();
                _job.total_deletes++;
                self.eventCounts[CMD_DELETE]++;
                sender.send("DELETED");
            } else {
                newlist.push(_job);
            }
        });

        if (!found){
            sender.send("NOT FOUND");
        } else {
            self.jobs_list = newlist;
        }
    };

    self.getJobsForClient = function(sender, state)
    {
        if (state == undefined){
            state = BeanJobModule.JOBSTATE_READY;
        }

        // get the list of jobs in the watched tubes
        var watched_jobs = _(self.jobs_list).chain()
            .filter(function(_job){
                var tube = self.findTube(_job.tube);
                return _.contains(sender.watching, _job.tube)   // client is watching the tube
                        && !tube.paused
                        && _job.state == state;            // job is ready
            })
            .sortBy('timeout_at')
            .reverse()
            .sortBy('priority')
            .value();

        return watched_jobs;
    };

    self.commandReserve = function(sender)
    {
        var watched_jobs = self.getJobsForClient(sender);

        if (watched_jobs.length > 0){
            self.eventCounts[CMD_RESERVE]++;
            sender.isWorker = true;
            watched_jobs[0].reserve(sender);
        } else {
            sender.send("TIMED OUT");
        }
    };

    self.commandReserveTimeout = function(sender, reserve_timeout)
    {
        var watched_jobs = self.getJobsForClient(sender);

        if (watched_jobs.length > 0){
            self.eventCounts[CMD_RESERVE]++;
            watched_jobs[0].reserve(sender);
        } else if (reserve_timeout > 0){
            sender.reserving = true;
            sender.reserving_until = moment().add(reserve_timeout, 'seconds');
        } else {
            sender.send("TIMED OUT");
        }
    };

    self.commandRelease = function(sender, job_id, priority, delay)
    {
        // set the status on the job
        var job = self.findJob(job_id);
        if (job != null) {
            self.eventCounts[CMD_RELEASE]++;
            job.priority = priority;
            job.release(delay);
            sender.send("RELEASED");
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandBury = function(sender, job_id, priority)
    {
        // set the status on the job
        var job = self.findJob(job_id);
        job.priority = priority;
        if (job != null) {
            self.eventCounts[CMD_BURY]++;
            job.bury();
            sender.send("BURIED");
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandTouch = function(sender, job_id)
    {
        // set the timeout on the job
        var job = self.findJob(job_id);
        if (job != null) {
            job.touch();
            sender.send("TOUCHED");
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandPeekId = function(sender, job_id)
    {
        var job = self.findJob(job_id);
        if (job != null) {
            var msg = "FOUND "+job_id+" "+job.data.length+"\r\n"+job.data;
            sender.send(msg);
            self.eventCounts[CMD_PEEK]++;
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandPeekState = function(sender, state)
    {
        // peeks the next job in the current queue
        var job = self.getJobsForClient(sender, state);
        if (job != null){
            var msg = "FOUND "+job_id+" "+job.data.length+"\r\n"+job.data;
            sender.send(msg);

            if (state == 'buried') {
                self.eventCounts[CMD_PEEK_BURIED]++;
            } else if (state == 'delayed'){
                self.eventCounts[CMD_PEEK_DELAYED]++;
            } else {
                self.eventCounts[CMD_PEEK_READY]++;
            }
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandKickJob = function(sender, job_id)
    {
        // kicks a specific job
        var job = self.findJob(job_id);
        if (job != null){
            job.kick();
            sender.send('KICKED');
            self.eventCounts[CMD_KICK]++;
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandPauseTube = function(sender, tube_name, delay_seconds)
    {
        var tube = self.findTube(tube_name);
        if (tube != null){
            tube.paused = true;
            tube.pause_start = moment();
            tube.pause_until = moment().add(delay_seconds, 'seconds');
            tube.total_pauses++;
            sender.send('PAUSED');
            self.eventCounts[CMD_PAUSE_TUBE]++;
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandListTubes = function(sender)
    {
        // faking some yaml here
        var msg = "";

        _.forEach(self.tubes, function(_tube){
            msg += "- "+_tube+"\r\n";
        });

        msg = "OK "+msg.length+"\r\n" + msg;
        self.send(msg);
        processor.eventCounts[CMD_LIST_TUBES]++;
    };

    self.commandKickTube = function(sender, tube_name, count)
    {
        // kicks the tube
        var tube = self.findTube(tube_name);

        var buried_jobs = _(watched_jobs).chain()
            .filter(self.jobs_list, function(_job){
                var tube = self.findTube(_job.tube);
                return tube_name == _job.tube
                    && _job.state == BeanJobModule.JOBSTATE_BURIED;
            })
            .sortBy('timeout_at')
            .reverse()
            .sortBy('priority')
            .value();

        var delayed_jobs = _(watched_jobs).chain()
            .filter(self.jobs_list, function(_job){
                var tube = self.findTube(_job.tube);
                return tube_name == _job.tube
                    && _job.state == BeanJobModule.JOBSTATE_DELAYED;
            })
            .sortBy('timeout_at')
            .reverse()
            .sortBy('priority')
            .value();

        if (buried_jobs.length > 0){
            for(var idx = 0; idx < count; idx++){
                buried_jobs[idx].kick();
                self.eventCounts[CMD_KICK]++;
            }
        } else if (delayed_jobs.length > 0){
            for(var idx = 0; idx < count; idx++){
                delayed_jobs[idx].kick();
                self.eventCounts[CMD_KICK]++;
            }
        }
    };

    self.commandStatsJob = function(sender, job_id)
    {
        // stats for a specific job
        var job = self.findJob(job_id);
        if (job != null){

            var msg = "---\n";
            msg += "- id: "+job_id+"\r\n";
            msg += "- tube: "+job.tube+"\r\n";

            var state_string = "ready";
            if (job.state == BeanJobModule.JOBSTATE_DELAYED) state_string = "delayed";
            else if (job.state == BeanJobModule.JOBSTATE_RESERVED) state_string = "reserved";
            else if (job.state == BeanJobModule.JOBSTATE_BURIED) state_string = "buried";
            msg += "- state: "+state_string+"\r\n";

            msg += "- pri: "+job.priority+"\r\n";

            var startmoment = moment(job.id);
            var nowmoment = moment();
            var age = nowmoment.diff(startmoment, "seconds");
            msg += "- age: "+age+"\r\n";

            var timeleft = nowmoment.diff(job.delay_until, "seconds");
            msg += "- age: "+timeleft+"\r\n";

            msg += "- file: 0\r\n";

            msg += "- reserves: "+job.eventCounts[BeanClientModule.BeanJob.counter.RESERVES]+"\r\n";
            msg += "- timeouts: "+job.eventCounts[BeanClientModule.BeanJob.counter.TIMEOUTS]+"\r\n";
            msg += "- releases: "+job.eventCounts[BeanClientModule.BeanJob.counter.RELEASES]+"\r\n";
            msg += "- buries: "+job.eventCounts[BeanClientModule.BeanJob.counter.BURIES]+"\r\n";
            msg += "- kicks: "+job.eventCounts[BeanClientModule.BeanJob.counter.KICKS]+"\r\n";

            msg = "OK "+msg.length+"\r\n"+msg;
            sender.send(msg);

            self.eventCounts[CMD_STATS_JOB]++;
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandStatsTube = function(sender, tube_name)
    {
        // stats for a tube
        var tube = self.findTube(tube_name);
        if (job != null){
            var msg = "---\n";
            msg += "- name: "+tube.name+"\r\n";

            var list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.priority < 1024; });
            msg += "- current-jobs-urgent: "+list.length+"\r\n";

            list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.status == BeanJobModule.JOBSTATE_READY; });
            msg += "- current-jobs-ready: "+list.length+"\r\n";

            list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.status == BeanJobModule.JOBSTATE_RESERVED; });
            msg += "- current-jobs-reserved: "+list.length+"\r\n";

            list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.status == BeanJobModule.JOBSTATE_DELAYED; });
            msg += "- current-jobs-delayed: "+list.length+"\r\n";

            list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.status == BeanJobModule.JOBSTATE_BURIED; });
            msg += "- current-jobs-buried: "+list.length+"\r\n";

            msg += "- total-jobs: "+tube.total_jobs+"\r\n";

            list = _.find(bean_clients, function(_client){ return _client.tube == tube.name; });
            msg += "- current-using: "+list.length+"\r\n";

            list = _.find(bean_clients, function(_client){ return _client.reserving; });
            msg += "- current-waiting: "+list.length+"\r\n";

            list = _.find(bean_clients, function(_client){ return _.indexOf(_client.watching, tube.name); });
            msg += "- current-watching: "+list.length+"\r\n";

            var startmoment = moment(tube.pause_start);
            var nowmoment = moment();
            var pause_seconds = nowmoment.diff(startmoment, "seconds");
            msg += "- pause: "+pause_seconds+"\r\n";

            msg += "- cmd-delete: "+tube.total_deletes+"\r\n";

            msg += "- cmd-pause-tube: "+tube.total_pauses+"\r\n";

            startmoment = moment();
            nowmoment = moment(tube.pause_until);
            pause_seconds = nowmoment.diff(startmoment, "seconds");
            msg += "- pause-time-left: "+pause_seconds+"\r\n";

            msg = "OK "+msg.length+"\r\n"+msg;
            sender.send(msg);

            self.eventCounts[CMD_STATS_TUBE]++;
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandStatsServer = function(sender)
    {
        var msg = "---\n";

        var list = _.find(self.jobs_list, function(_job){ return _job.priority < 1024; });
        msg += "- current-jobs-urgent: "+list.length+"\r\n";

        list = _.find(self.jobs_list, function(_job){ return _job.status == BeanJobModule.JOBSTATE_READY; });
        msg += "- current-jobs-ready: "+list.length+"\r\n";

        list = _.find(self.jobs_list, function(_job){ return _job.status == BeanJobModule.JOBSTATE_RESERVED; });
        msg += "- current-jobs-reserved: "+list.length+"\r\n";

        list = _.find(self.jobs_list, function(_job){ return _job.status == BeanJobModule.JOBSTATE_DELAYED; });
        msg += "- current-jobs-delayed: "+list.length+"\r\n";

        list = _.find(self.jobs_list, function(_job){ return _job.status == BeanJobModule.JOBSTATE_BURIED; });
        msg += "- current-jobs-buried: "+list.length+"\r\n";

        msg += "- cmd-put: "+self.eventCounts[CMD_PUT]+"\r\n";
        msg += "- cmd-peek: "+self.eventCounts[CMD_PEEK]+"\r\n";
        msg += "- cmd-peek-ready: "+self.eventCounts[CMD_PEEK_READY]+"\r\n";
        msg += "- cmd-peek-delayed: "+self.eventCounts[CMD_PEEK_DELAYED]+"\r\n";
        msg += "- cmd-peek-buried: "+self.eventCounts[CMD_PEEK_BURIED]+"\r\n";
        msg += "- cmd-reserve: "+self.eventCounts[CMD_RESERVE]+"\r\n";
        msg += "- cmd-use: "+self.eventCounts[CMD_USE]+"\r\n";
        msg += "- cmd-watch: "+self.eventCounts[CMD_WATCH]+"\r\n";
        msg += "- cmd-ignore: "+self.eventCounts[CMD_IGNORE]+"\r\n";
        msg += "- cmd-delete: "+self.eventCounts[CMD_DELETE]+"\r\n";
        msg += "- cmd-release: "+self.eventCounts[CMD_RELEASE]+"\r\n";
        msg += "- cmd-bury: "+self.eventCounts[CMD_BURY]+"\r\n";
        msg += "- cmd-kick: "+self.eventCounts[CMD_KICK]+"\r\n";
        msg += "- cmd-stats: "+self.eventCounts[CMD_STATS]+"\r\n";
        msg += "- cmd-stats-job: "+self.eventCounts[CMD_STATS_JOB]+"\r\n";
        msg += "- cmd-stats-tube: "+self.eventCounts[CMD_STATS_TUBE]+"\r\n";
        msg += "- cmd-list-tubes: "+self.eventCounts[CMD_LIST_TUBES]+"\r\n";
        msg += "- cmd-list-tube-used: "+self.eventCounts[CMD_LIST_TUBE_USED]+"\r\n";
        msg += "- cmd-list-tubes-watched: "+self.eventCounts[CMD_LIST_TUBES_WATCHED]+"\r\n";
        msg += "- cmd-pause-tube: "+self.eventCounts[CMD_PAUSE_TUBE]+"\r\n";
        msg += "- job-timeouts: "+self.eventCounts[CMD_JOB_TIMEOUT]+"\r\n";
        msg += "- total-jobs: "+self.eventCounts[CMD_TOTAL_JOBS]+"\r\n";

        var largest_job = _.max(self.jobs_list, function(_job){ return _job.data.length; });
        msg += "- max-job-size: "+largest_job.data.length+"\r\n";

        msg += "- current-tubes: "+self.tubes.length+"\r\n";

        msg += "- current-connections: "+bean_clients.length+"\r\n";

        var producers = _.findWhere(bean_clients, {isProducer: true});
        msg += "- current-producers: "+producers.length+"\r\n";

        var workers = _.findWhere(bean_clients, {isWorker: true});
        msg += "- current-workers: "+workers.length+"\r\n";

        var waiting = _.findWhere(bean_clients, {reserving: true});
        msg += "- current-waiting: "+waiting.length+"\r\n";

        msg += "- pid: 0\r\n";
        msg += "- version: 0.1\r\n";
        msg += "- rusage-utime: 0\r\n";
        msg += "- rusage-stime: 0\r\n";
        msg += "- uptime: 0\r\n";
        msg += "- binlog-oldest-index: 0\r\n";
        msg += "- binlog-oldest-index: 0\r\n";
        msg += "- binlog-current-index: 0\r\n";
        msg += "- binlog-max-size: 0\r\n";
        msg += "- binlog-records-written: 0\r\n";
        msg += "- binlog-records-migrated: 0\r\n";
        msg += "- id: "+self.id+"\r\n";
        msg += "- pid: 0\r\n";

        var os = require("os");
        msg += "- hostname: "+os.hostname()+"\r\n";

        msg = "OK "+msg.length+"\r\n"+msg;
        sender.send(msg);

        self.eventCounts[CMD_STATS]++;
    };
};

exports.BeanProcessor = BeanProcessor;
