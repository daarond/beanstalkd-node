/**
 * Created by daarond on 9/15/2015.
 */
var BeanClientModule = require('../beanstalk_client');
var BeanProcessorModule = require('../beanstalk_processor');
var BeanTubeModule = require('../beanstalk_tube');
var BeanJobModule = require('../beanstalk_job');
var sinon = require('sinon');
var fs = require('fs');

var assert = require('assert');

var job_exists = function(job_id){
    fs.access('../jobs/' + job_id + '.bsjob', fs.R_OK | fs.W_OK, function (err) {
        return !err;
    });
};


suite('Client', function()
{
    var incoming_data = [];

    beforeEach(function() {
        incoming_data = [];
    });

    var createProcessorWithTube = function(tube_name)
    {
        var proc = new BeanProcessorModule.BeanProcessor();
        var tube = new BeanTubeModule.Tube();
        tube.name = 'testtube';
        proc.tubes.push(tube);
        return proc;
    };

    var addJob = function(processor, client)
    {
        var job = new BeanJobModule.BeanJob(client, 'testtube', 1000, 30, 0);
        job.state = BeanJobModule.JOBSTATE_READY;
        job.data = 'this is data';
        processor.jobs_list.push(job);
        return job;
    };

    var createClient = function(tube, proc)
    {
        var client = new BeanClientModule.BeanClient(null, proc);
        sinon.stub(client, "send", function (data) {
            incoming_data.push(data + "\r\n");
        });
        client.tube = tube;
        client.watching.push(tube);
        return client;
    };


    /*
     /$$$$$$$  /$$   /$$ /$$$$$$$$
     | $$__  $$| $$  | $$|__  $$__/
     | $$  \ $$| $$  | $$   | $$
     | $$$$$$$/| $$  | $$   | $$
     | $$____/ | $$  | $$   | $$
     | $$      | $$  | $$   | $$
     | $$      |  $$$$$$/   | $$
     |__/       \______/    |__/

     */
    suite('Put', function()
    {

        test('put with new tube', function()
        {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 "+data.length+"\r\n"+data+"\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            // should tell the user it was inserted
            assert.equal(1, incoming_data.length);

            var myregexp = /^INSERTED (\d+)\r\n$/i;
            var match = myregexp.exec(incoming_data[0]);

            assert(match != null, "did not receive well-formatted inserted message");

            // should be in the jobs list
            assert.equal(1, proc.jobs_list.length, "does not exist in jobs list");

            var job = proc.jobs_list[0];
            assert.equal(match[1], job.id, "job id does not match");

            assert.equal(1, proc.tubes.length, "tube not created");
        });


        test('put with existing tube', function()
        {
            var proc = new BeanProcessorModule.BeanProcessor();

            var tube = new BeanTubeModule.Tube();
            tube.name = 'testtube';
            proc.tubes.push(tube);

            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 "+data.length+"\r\n"+data+"\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            assert.equal(1, proc.tubes.length, "more tubes created");
        });

        test('put with bad tube', function()
        {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('*', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 "+data.length+"\r\n"+data+"\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            assert.equal('BAD_FORMAT\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
        });

        test('put with negative priority', function()
        {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put -1 0 30 "+data.length+"\r\n"+data+"\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            assert.equal('BAD_FORMAT\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
        });

        test('put with bad length', function()
        {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 1"+data.length+"\r\n"+data+"\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            assert.equal('EXPECTED_CRLF\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
        });

        test('put while draining', function()
        {
            var proc = new BeanProcessorModule.BeanProcessor();
            proc.drain_mode = true;
            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 "+data.length+"\r\n"+data+"\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            assert.equal('DRAINING\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
        });
    });





/*
 /$$$$$$$  /$$$$$$$$ /$$       /$$$$$$$$ /$$$$$$$$ /$$$$$$$$
 | $$__  $$| $$_____/| $$      | $$_____/|__  $$__/| $$_____/
 | $$  \ $$| $$      | $$      | $$         | $$   | $$
 | $$  | $$| $$$$$   | $$      | $$$$$      | $$   | $$$$$
 | $$  | $$| $$__/   | $$      | $$__/      | $$   | $$__/
 | $$  | $$| $$      | $$      | $$         | $$   | $$
 | $$$$$$$/| $$$$$$$$| $$$$$$$$| $$$$$$$$   | $$   | $$$$$$$$
 |_______/ |________/|________/|________/   |__/   |________/

 */
    suite('Delete', function() {

        test('delete existing message', function ()
        {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var job = new BeanJobModule.BeanJob(client, 'testtube', 1000, 30, 0);
            proc.jobs_list.push(job);

            client.dataReceived("delete "+job.id+"\r\n");
            proc.processCommandList();

            assert(/^DELETED\r\n$/.test(incoming_data[0]), "did not receive a delete message");

            assert.equal(0, proc.jobs_list.length, "still exists in the list");
        });

        test('delete non-existing message', function ()
        {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var job = new BeanJobModule.BeanJob(client, 'testtube', 1000, 30, 0);
            proc.jobs_list.push(job);

            client.dataReceived("delete 1\r\n");
            proc.processCommandList();

            assert(/^NOT FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });





/*
 /$$$$$$$  /$$$$$$$$  /$$$$$$  /$$$$$$$$ /$$$$$$$  /$$    /$$ /$$$$$$$$
 | $$__  $$| $$_____/ /$$__  $$| $$_____/| $$__  $$| $$   | $$| $$_____/
 | $$  \ $$| $$      | $$  \__/| $$      | $$  \ $$| $$   | $$| $$
 | $$$$$$$/| $$$$$   |  $$$$$$ | $$$$$   | $$$$$$$/|  $$ / $$/| $$$$$
 | $$__  $$| $$__/    \____  $$| $$__/   | $$__  $$ \  $$ $$/ | $$__/
 | $$  \ $$| $$       /$$  \ $$| $$      | $$  \ $$  \  $$$/  | $$
 | $$  | $$| $$$$$$$$|  $$$$$$/| $$$$$$$$| $$  | $$   \  $/   | $$$$$$$$
 |__/  |__/|________/ \______/ |________/|__/  |__/    \_/    |________/
 */
    suite.only('Reserve', function() {

        test('basic reserve', function ()
        {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client);

            client.dataReceived("reserve\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");
            assert(/^RESERVED \d+ \d+$/.test(result[0]), "did not receive a reserved message");
            assert(/^this is data$/.test(result[1]), "data text is different");

            assert(job.state==BeanJobModule.JOBSTATE_RESERVED, "not marked reserved");
        });

        test('reserve no message', function ()
        {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("reserve\r\n");
            proc.processCommandList();

            assert(/^TIMED OUT\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });
});
