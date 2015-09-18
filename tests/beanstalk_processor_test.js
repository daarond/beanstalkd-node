/**
 * Created by daarond on 9/11/2015.
 */

var BeanClientModule = require('../beanstalk_client');
var BeanProcessorModule = require('../beanstalk_processor');
var BeanTubeModule = require('../beanstalk_tube');
var BeanJobModule = require('../beanstalk_job');
var sinon = require('sinon');
var assert = require('assert');

suite('Processor', function() {
    var incoming_data = [];

    beforeEach(function () {
        incoming_data = [];
    });

    var createProcessorWithTube = function (tube_name) {
        var proc = new BeanProcessorModule.BeanProcessor();
        var tube = new BeanTubeModule.Tube();
        tube.name = 'testtube';
        proc.tubes.push(tube);
        return proc;
    };

    var addJob = function (processor, client, state) {
        var job = new BeanJobModule.BeanJob(client, 'testtube', 1000, 30, 0);
        job.state = state;
        job.data = 'this is data';
        processor.jobs_list.push(job);
        return job;
    };

    var createClient = function (tube, proc) {
        var client = new BeanClientModule.BeanClient(null, proc);
        sinon.stub(client, "send", function (data) {
            incoming_data.push(data + "\r\n");
        });
        client.tube = tube;
        client.watching.push(tube);
        proc.bean_clients.push(client);
        return client;
    };

    suite('Delete unused tubes', function () {
        test('unused by anyone', function () {
            var proc = createProcessorWithTube('testtube');
            proc.deleteUnusedTubes();
            assert(proc.tubes.length == 0, "a tube still exists");
        });

        test('used as tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            proc.deleteUnusedTubes();
            assert(proc.tubes.length == 1, "tube does not exist");
        });

        test('used as watching', function () {
            var proc = createProcessorWithTube('testtube');
            var tube = new BeanTubeModule.Tube();
            tube.name = 'another';
            proc.tubes.push(tube);
            var client = createClient('testtube', proc);
            client.watching.push('another');

            proc.deleteUnusedTubes();
            assert(proc.tubes.length == 2, "tube does not exist");
        });

        test('in delayed job', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var msg = "data information";
            proc.commandPut(client, 'another', 1000, 0, 30, msg.length, msg);

            proc.deleteUnusedTubes();
            assert(proc.tubes.length == 2, "tube does not exist");
        });
    });
});
