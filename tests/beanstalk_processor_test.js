/**
 * Created by daarond on 9/11/2015.
 */

var BeanProcessorModule = require('../beanstalk_processor');
var BeanClientModule = require('../beanstalk_client');
var sinon = require('sinon');
var fs = require('fs');

var assert = require('assert');

var job_exists = function(job_id){
    fs.access('../jobs/' + job_id + '.bsjob', fs.R_OK | fs.W_OK, function (err) {
        return !err;
    });
};


suite('Processor', function() {

    suite('Processor Put', function() {

        test('put with new tube', function() {
            var proc = new BeanProcessorModule.BeanProcessor();

            var incoming_data = [];
            var client = new BeanClientModule.BeanClient(null);
            sinon.stub(client, "send", function (data) {
                incoming_data.push(data + "\r\n");
            });

            var data = 'this is the text\r\n';
            proc.commandPut(client, 'starting_tube', 1, 0, 60, data.length, data);

            // should tell the user it was inserted
            assert.equal(1, incoming_data.length);

            var myregexp = /^INSERTED (\d+)\r\n$/i;
            var match = myregexp.exec(incoming_data[0]);

            assert(match != null, "did not receive well-formatted inserted message");

            // should be in the jobs list
            assert.equal(1, proc.jobs_list.length, "does not exist in jobs list");

            var job = proc.jobs_list[0];
            assert.equal(match[1], job.id, "job id does not match");

            // should have a file created
            fs.access('../jobs/' + job.id + '.bsjob', fs.R_OK | fs.W_OK, function (err) {
                if (err) {
                    assert.fail("no access to job file " + job.id + ".bsjob");
                }
            });

            // clean up the file
            job.delete();
        });

        test('put with existing tube', function() {
            var proc = new BeanProcessorModule.BeanProcessor();

            var incoming_data = [];
            var client = new BeanClientModule.BeanClient(null);
            sinon.stub(client, "send", function (data) {
                incoming_data.push(data + "\r\n");
            });

            var starting_tube_count = proc.tubes.length;

            var data = 'this is the text\r\n';
            proc.commandPut(client, 'starting_tube', 1, 0, 60, data.length, data);

            assert.equal(starting_tube_count+1, proc.tubes.length);
            assert.equal('starting_tube', proc.tubes[0].name);
            assert.equal(1, proc.jobs_list.length, "job not found");

            // clean up the file
            proc.jobs_list[0].delete();
        });

        test('put with bad tube', function() {
            var proc = new BeanProcessorModule.BeanProcessor();

            var incoming_data = [];
            var client = new BeanClientModule.BeanClient(null);
            sinon.stub(client, "send", function (data) {
                incoming_data.push(data + "\r\n");
            });

            var starting_tube_count = proc.tubes.length;

            var data = 'this is the text\r\n';
            proc.commandPut(client, '*', 1, 0, 60, data.length, data);

            assert.equal('BAD_FORMAT\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
            assert.equal(starting_tube_count, proc.tubes.length);
        });

        test('put with negative priority', function() {
            var proc = new BeanProcessorModule.BeanProcessor();

            var incoming_data = [];
            var client = new BeanClientModule.BeanClient(null);
            sinon.stub(client, "send", function (data) {
                incoming_data.push(data + "\r\n");
            });

            var starting_tube_count = proc.tubes.length;

            var data = 'this is the text\r\n';
            proc.commandPut(client, 'tube', -1, 0, 60, data.length, data);

            assert.equal('BAD_FORMAT\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
            assert.equal(starting_tube_count, proc.tubes.length);
        });

        test('put with bad length', function() {
            var proc = new BeanProcessorModule.BeanProcessor();

            var incoming_data = [];
            var client = new BeanClientModule.BeanClient(null);
            sinon.stub(client, "send", function (data) {
                incoming_data.push(data + "\r\n");
            });

            var starting_tube_count = proc.tubes.length;

            var data = 'this is the text\r\n';
            proc.commandPut(client, 'tube', 1, 0, 60, data.length+3, data);

            assert.equal('EXPECTED_CRLF\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
            assert.equal(starting_tube_count, proc.tubes.length);
        });

        test('put while draining', function() {
            var proc = new BeanProcessorModule.BeanProcessor();
            proc.drain_mode = true;

            var incoming_data = [];
            var client = new BeanClientModule.BeanClient(null);
            sinon.stub(client, "send", function (data) {
                incoming_data.push(data + "\r\n");
            });

            var data = 'this is the text\r\n';
            proc.commandPut(client, 'tube', -1, 0, 60, data.length, data);

            assert.equal('DRAINING\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
        });
    });





    suite('Processor Delete', function() {

        test('delete existing message', function () {
            var proc = new BeanProcessorModule.BeanProcessor();

            var incoming_data = [];
            var client = new BeanClientModule.BeanClient(null);
            sinon.stub(client, "send", function (data) {
                incoming_data.push(data + "\r\n");
            });

            var data = 'this is the text\r\n';
            proc.commandPut(client, 'starting_tube', 1, 0, 60, data.length, data);

            // reset the array to make it clear
            incoming_data = [];

            proc.commandDelete(client, proc.jobs_list[0].id);

            assert(/^DELETED\r\n$/.test(incoming_data[0]), "did not receive a delete message");

            assert.equal(0, proc.jobs_list.length, "still exists in the list");
        });

        test('delete non-existing message', function () {
            var proc = new BeanProcessorModule.BeanProcessor();

            var incoming_data = [];
            var client = new BeanClientModule.BeanClient(null);
            sinon.stub(client, "send", function (data) {
                incoming_data.push(data + "\r\n");
            });

            // reset the array to make it clear
            incoming_data = [];

            proc.commandDelete(client, 1); // in a unix epoch, this job id should not exist

            assert(/^NOT FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });
});
