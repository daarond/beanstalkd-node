/**
 * Created by daarond on 9/9/2015.
 */

var BeanTubeModule = require('./beanstalk_tube');
var BeanJobModule = require('./beanstalk_job');
var BeanClientModule = require('./beanstalk_client');
var BeanProcessorModule = require('./beanstalk_processor');
var yaml = require('js-yaml');
var moment = require("moment");
var _ = require("underscore");

exports.CMD_PUT = 0;
exports.CMD_PEEK = 1;
exports.CMD_PEEK_READY = 2;
exports.CMD_PEEK_DELAYED = 3;
exports.CMD_PEEK_BURIED = 4;
exports.CMD_RESERVE = 5;
exports.CMD_USE = 6;
exports.CMD_WATCH = 7;
exports.CMD_IGNORE = 8;
exports.CMD_DELETE = 9;
exports.CMD_RELEASE = 10;
exports.CMD_BURY = 11;
exports.CMD_KICK = 12;
exports.CMD_STATS = 13;
exports.CMD_STATS_JOB = 14;
exports.CMD_STATS_TUBE = 15;
exports.CMD_LIST_TUBES = 16;
exports.CMD_LIST_TUBE_USED = 17;
exports.CMD_LIST_TUBES_WATCHED = 18;
exports.CMD_PAUSE_TUBE = 19;
exports.CMD_JOB_TIMEOUT = 20;
exports.CMD_TOTAL_JOBS = 21;
exports.CMD_TOTAL_CONNECTIONS = 22;
exports.CMD_TOUCH = 23;

// https://raw.githubusercontent.com/kr/beanstalkd/master/doc/protocol.txt

var BeanProcessor = function()
{
    var self = this;
    self.bean_clients = [];
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
            self.commandPeekId(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'peek-ready'){
            self.commandPeekState(bean_command.client, BeanJobModule.JOBSTATE_READY);
        } else if (command == 'peek-delayed'){
            self.commandPeekState(bean_command.client, BeanJobModule.JOBSTATE_DELAYED);
        } else if (command == 'peek-buried'){
            self.commandPeekState(bean_command.client, BeanJobModule.JOBSTATE_BURIED);
        } else if (command == 'kick'){
            self.commandKickTube(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'kick-job'){
            self.commandKickJob(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'stats-job'){
            self.commandStatsJob(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'stats-tube'){
            self.commandStatsTube(bean_command.client, bean_command.commandline[1]);
        } else if (command == 'stats'){
            self.commandStats(bean_command.client);
        } else if (command == 'list-tubes'){
            self.commandListTubes(bean_command.client);
        } else if (command == 'pause-tube'){
            self.commandPauseTube(bean_command.client, bean_command.commandline[1], bean_command.commandline[2]);
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
                self.eventCounts[BeanProcessorModule.CMD_JOB_TIMEOUT]++;
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
                && _tube.pause_until.isBefore()) {
                _tube.paused = false;
            }
        });
    };

    self.checkClientReserves = function()
    {
        _.forEach(self.bean_clients, function(_client){
            if (_client.reserving
            && _client.reserving_until.isAfter()) {
                var job = self.getJobForClient(_client);
                if (job != null){
                    _client.isWorker = true;
                    job.reserve(_client);
                }
            } else if (_client.reserving
                && _client.reserving_until.isBefore()) {
                _client.send('TIMED_OUT');
            }
        });
    };

    self.deleteUnusedTubes = function()
    {
        // get the list of tubes used by jobs
        var used_list = _.pluck(self.jobs_list, 'tube');

        // get the list of tubes used by clients
        used_list = used_list.concat(_.pluck(self.bean_clients, 'tube'));

        _.each(self.bean_clients, function(_client){
            used_list = used_list.concat(_client.watching);
        });

        used_list = _.uniq(used_list);

        _.forEach(self.tubes, function(_tube){
            if (_.indexOf(used_list, _tube.name) == -1){
                self.tubes = _.reject(self.tubes, function(unused_tube){ return unused_tube.name == _tube.name });
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
        if (tube == null && !self.isBadTubeName(tube_name)){
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
        if (self.findJob(job.id) != null){
            job.assignId();
        }
        if (delay > 0){
            job.state = BeanJobModule.JOBSTATE_DELAYED;
        }
        job.data = data;
        job.updateFile();
        self.jobs_list.push(job);

        self.eventCounts[BeanProcessorModule.CMD_PUT]++;
        self.eventCounts[BeanProcessorModule.CMD_TOTAL_JOBS]++;
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
                self.eventCounts[BeanProcessorModule.CMD_DELETE]++;
                sender.send("DELETED");
            } else {
                newlist.push(_job);
            }
        });

        if (!found){
            sender.send("NOT_FOUND");
        } else {
            self.jobs_list = newlist;
        }
    };

    self.getJobForClient = function(sender, state)
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

        if (watched_jobs.length>0){
            return watched_jobs[0];
        } else {
            return null;
        }
    };

    self.commandReserve = function(sender)
    {
        var job = self.getJobForClient(sender);

        if (job != null){
            self.eventCounts[BeanProcessorModule.CMD_RESERVE]++;
            sender.isWorker = true;
            job.reserve(sender);
        } else {
            sender.send("TIMED_OUT");
        }
    };

    self.commandReserveTimeout = function(sender, reserve_timeout)
    {
        var job = self.getJobForClient(sender);

        if (job != null){
            // there are already jobs available, so get one of those
            self.eventCounts[BeanProcessorModule.CMD_RESERVE]++;
            job.reserve(sender);
        } else if (reserve_timeout > 0){
            // no jobs, but a positive timeout
            sender.reserving = true;
            sender.reserving_until = moment().add(reserve_timeout, 'seconds');
        } else {
            // set for a zero timeout, so time it out already
            sender.send("TIMED_OUT");
        }
    };

    self.commandRelease = function(sender, job_id, priority, delay)
    {
        // set the status on the job
        var job = self.findJob(job_id);
        if (job != null) {
            self.eventCounts[BeanProcessorModule.CMD_RELEASE]++;
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

        if (job != null) {
            job.priority = priority;
            self.eventCounts[BeanProcessorModule.CMD_BURY]++;
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
            self.eventCounts[BeanProcessorModule.CMD_PEEK]++;
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandPeekState = function(sender, state)
    {
        // peeks the next job in the current queue
        var job = self.getJobForClient(sender, state);
        if (job != null){
            var msg = "FOUND "+job.id+" "+job.data.length+"\r\n"+job.data;
            sender.send(msg);

            if (state == 'buried') {
                self.eventCounts[BeanProcessorModule.CMD_PEEK_BURIED]++;
            } else if (state == 'delayed'){
                self.eventCounts[BeanProcessorModule.CMD_PEEK_DELAYED]++;
            } else {
                self.eventCounts[BeanProcessorModule.CMD_PEEK_READY]++;
            }
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandKickJob = function(sender, job_id)
    {
        // kicks a specific job
        var job = self.findJob(job_id);
        if (job != null
            && (job.state == BeanJobModule.JOBSTATE_BURIED || job.state == BeanJobModule.JOBSTATE_DELAYED)){
            job.kick();
            sender.send('KICKED');
            self.eventCounts[BeanProcessorModule.CMD_KICK]++;
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
            self.eventCounts[BeanProcessorModule.CMD_PAUSE_TUBE]++;
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandListTubes = function(sender)
    {
        var tubenames = _.pluck(self.tubes, 'name');
        var msg = msg = yaml.safeDump(tubenames);

        msg = "OK "+msg.length+"\r\n" + msg;
        sender.send(msg);
        self.eventCounts[BeanProcessorModule.CMD_LIST_TUBES]++;
    };

    self.commandKickTube = function(sender, count)
    {
        // kicks the tube
        var tube = self.findTube(sender.tube);
        var kick_count = count;

        // no tube found
        if (tube == null){
            sender.send("KICKED 0");
            return;
        }

        var buried_jobs = _(self.jobs_list).chain()
            .filter(function(_job){
                var tube = self.findTube(_job.tube);
                return sender.tube == _job.tube
                    && _job.state == BeanJobModule.JOBSTATE_BURIED;
            })
            .sortBy('timeout_at')
            .reverse()
            .sortBy('priority')
            .value();

        if (buried_jobs.length == 0) {
            var delayed_jobs = _(self.jobs_list).chain()
                .filter(function (_job) {
                    var tube = self.findTube(_job.tube);
                    return sender.tube == _job.tube
                        && _job.state == BeanJobModule.JOBSTATE_DELAYED;
                })
                .sortBy('timeout_at')
                .reverse()
                .sortBy('priority')
                .value();

            if (kick_count > delayed_jobs.length){
                kick_count = delayed_jobs.length;
            }

            for(var idx = 0; idx < kick_count; idx++){
                var job = self.findJob(delayed_jobs[idx].id);
                job.kick();
                self.eventCounts[BeanProcessorModule.CMD_KICK]++;
            }
        } else {
            if (kick_count > buried_jobs.length){
                kick_count = buried_jobs.length;
            }

            for(var idx = 0; idx < kick_count; idx++){
                var job = self.findJob(buried_jobs[idx].id);
                job.kick();
                self.eventCounts[BeanProcessorModule.CMD_KICK]++;
            }
        }

        sender.send('KICKED '+kick_count);
    };

    self.commandStatsJob = function(sender, job_id)
    {
        // stats for a specific job
        var job = self.findJob(job_id);
        if (job != null){
            var stats = {};
            stats.id = job_id;
            stats.tube = job.tube;

            var state_string = "ready";
            if (job.state == BeanJobModule.JOBSTATE_DELAYED) state_string = "delayed";
            else if (job.state == BeanJobModule.JOBSTATE_RESERVED) state_string = "reserved";
            else if (job.state == BeanJobModule.JOBSTATE_BURIED) state_string = "buried";
            stats.state = state_string;

            stats.pri = job.priority;

            var startmoment = moment(job.id);
            var nowmoment = moment();
            stats.age = nowmoment.diff(startmoment, "seconds");
            stats.timeleft = nowmoment.diff(job.delay_until, "seconds");

            stats.file = 0;

            stats.reserves = job.eventCounts[BeanClientModule.BeanJob.counter.RESERVES];
            stats.timeouts = job.eventCounts[BeanClientModule.BeanJob.counter.TIMEOUTS];
            stats.releases = job.eventCounts[BeanClientModule.BeanJob.counter.RELEASES];
            stats.buries = job.eventCounts[BeanClientModule.BeanJob.counter.BURIES];
            stats.kicks = job.eventCounts[BeanClientModule.BeanJob.counter.KICKS];

            var msg = yaml.safeDump(stats);
            msg = "OK "+msg.length+"\r\n"+msg;
            sender.send(msg);

            self.eventCounts[BeanProcessorModule.CMD_STATS_JOB]++;
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandStatsTube = function(sender, tube_name)
    {
        // stats for a tube
        var tube = self.findTube(tube_name);
        if (job != null){
            msg += "- name: "+tube.name+"\n";

            var list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.priority < 1024; });
            msg += "- current-jobs-urgent: "+list.length+"\n";

            list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.status == BeanJobModule.JOBSTATE_READY; });
            msg += "- current-jobs-ready: "+list.length+"\n";

            list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.status == BeanJobModule.JOBSTATE_RESERVED; });
            msg += "- current-jobs-reserved: "+list.length+"\n";

            list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.status == BeanJobModule.JOBSTATE_DELAYED; });
            msg += "- current-jobs-delayed: "+list.length+"\n";

            list = _.find(self.jobs_list, function(_job){ return _job.tube==tube_name && _job.status == BeanJobModule.JOBSTATE_BURIED; });
            msg += "- current-jobs-buried: "+list.length+"\n";

            msg += "- total-jobs: "+tube.total_jobs+"\n";

            list = _.find(self.bean_clients, function(_client){ return _client.tube == tube.name; });
            msg += "- current-using: "+list.length+"\n";

            list = _.find(self.bean_clients, function(_client){ return _client.reserving; });
            msg += "- current-waiting: "+list.length+"\n";

            list = _.find(self.bean_clients, function(_client){ return _.indexOf(_client.watching, tube.name); });
            msg += "- current-watching: "+list.length+"\n";

            var startmoment = moment(tube.pause_start);
            var nowmoment = moment();
            var pause_seconds = nowmoment.diff(startmoment, "seconds");
            msg += "- pause: "+pause_seconds+"\n";

            msg += "- cmd-delete: "+tube.total_deletes+"\n";

            msg += "- cmd-pause-tube: "+tube.total_pauses+"\n";

            startmoment = moment();
            nowmoment = moment(tube.pause_until);
            pause_seconds = nowmoment.diff(startmoment, "seconds");
            msg += "- pause-time-left: "+pause_seconds+"\n";

            msg = "OK "+msg.length+"\r\n"+msg;
            sender.send(msg);

            self.eventCounts[BeanProcessorModule.CMD_STATS_TUBE]++;
        } else {
            sender.send("NOT_FOUND");
        }
    };

    self.commandStatsServer = function(sender)
    {
        var msg = "";

        var list = _.find(self.jobs_list, function(_job){ return _job.priority < 1024; });
        msg += "- current-jobs-urgent: "+list.length+"\n";

        list = _.find(self.jobs_list, function(_job){ return _job.status == BeanJobModule.JOBSTATE_READY; });
        msg += "- current-jobs-ready: "+list.length+"\n";

        list = _.find(self.jobs_list, function(_job){ return _job.status == BeanJobModule.JOBSTATE_RESERVED; });
        msg += "- current-jobs-reserved: "+list.length+"\n";

        list = _.find(self.jobs_list, function(_job){ return _job.status == BeanJobModule.JOBSTATE_DELAYED; });
        msg += "- current-jobs-delayed: "+list.length+"\n";

        list = _.find(self.jobs_list, function(_job){ return _job.status == BeanJobModule.JOBSTATE_BURIED; });
        msg += "- current-jobs-buried: "+list.length+"\n";

        msg += "- cmd-put: "+self.eventCounts[BeanProcessorModule.CMD_PUT]+"\n";
        msg += "- cmd-peek: "+self.eventCounts[BeanProcessorModule.CMD_PEEK]+"\n";
        msg += "- cmd-peek-ready: "+self.eventCounts[BeanProcessorModule.CMD_PEEK_READY]+"\n";
        msg += "- cmd-peek-delayed: "+self.eventCounts[BeanProcessorModule.CMD_PEEK_DELAYED]+"\n";
        msg += "- cmd-peek-buried: "+self.eventCounts[BeanProcessorModule.CMD_PEEK_BURIED]+"\n";
        msg += "- cmd-reserve: "+self.eventCounts[BeanProcessorModule.CMD_RESERVE]+"\n";
        msg += "- cmd-use: "+self.eventCounts[BeanProcessorModule.CMD_USE]+"\n";
        msg += "- cmd-watch: "+self.eventCounts[BeanProcessorModule.CMD_WATCH]+"\n";
        msg += "- cmd-ignore: "+self.eventCounts[BeanProcessorModule.CMD_IGNORE]+"\n";
        msg += "- cmd-delete: "+self.eventCounts[BeanProcessorModule.CMD_DELETE]+"\n";
        msg += "- cmd-release: "+self.eventCounts[BeanProcessorModule.CMD_RELEASE]+"\n";
        msg += "- cmd-bury: "+self.eventCounts[BeanProcessorModule.CMD_BURY]+"\n";
        msg += "- cmd-kick: "+self.eventCounts[BeanProcessorModule.CMD_KICK]+"\n";
        msg += "- cmd-stats: "+self.eventCounts[BeanProcessorModule.CMD_STATS]+"\n";
        msg += "- cmd-stats-job: "+self.eventCounts[BeanProcessorModule.CMD_STATS_JOB]+"\n";
        msg += "- cmd-stats-tube: "+self.eventCounts[BeanProcessorModule.CMD_STATS_TUBE]+"\n";
        msg += "- cmd-list-tubes: "+self.eventCounts[BeanProcessorModule.CMD_LIST_TUBES]+"\n";
        msg += "- cmd-list-tube-used: "+self.eventCounts[BeanProcessorModule.CMD_LIST_TUBE_USED]+"\n";
        msg += "- cmd-list-tubes-watched: "+self.eventCounts[BeanProcessorModule.CMD_LIST_TUBES_WATCHED]+"\n";
        msg += "- cmd-pause-tube: "+self.eventCounts[BeanProcessorModule.CMD_PAUSE_TUBE]+"\n";
        msg += "- job-timeouts: "+self.eventCounts[BeanProcessorModule.CMD_JOB_TIMEOUT]+"\n";
        msg += "- total-jobs: "+self.eventCounts[BeanProcessorModule.CMD_TOTAL_JOBS]+"\n";

        var largest_job = _.max(self.jobs_list, function(_job){ return _job.data.length; });
        msg += "- max-job-size: "+largest_job.data.length+"\n";

        msg += "- current-tubes: "+self.tubes.length+"\n";

        msg += "- current-connections: "+self.bean_clients.length+"\n";

        var producers = _.findWhere(self.bean_clients, {isProducer: true});
        msg += "- current-producers: "+producers.length+"\n";

        var workers = _.findWhere(self.bean_clients, {isWorker: true});
        msg += "- current-workers: "+workers.length+"\n";

        var waiting = _.findWhere(self.bean_clients, {reserving: true});
        msg += "- current-waiting: "+waiting.length+"\n";

        msg += "- pid: 0\n";
        msg += "- version: 0.1\n";
        msg += "- rusage-utime: 0\n";
        msg += "- rusage-stime: 0\n";
        msg += "- uptime: 0\n";
        msg += "- binlog-oldest-index: 0\n";
        msg += "- binlog-oldest-index: 0\n";
        msg += "- binlog-current-index: 0\n";
        msg += "- binlog-max-size: 0\n";
        msg += "- binlog-records-written: 0\n";
        msg += "- binlog-records-migrated: 0\n";
        msg += "- id: "+self.id+"\n";
        msg += "- pid: 0\n";

        var os = require("os");
        msg += "- hostname: "+os.hostname()+"\n";

        msg = "OK "+msg.length+"\r\n"+msg;
        sender.send(msg);

        self.eventCounts[BeanProcessorModule.CMD_STATS]++;
    };
};

exports.BeanProcessor = BeanProcessor;
