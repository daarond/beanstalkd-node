/**
 * Created by daarond on 9/15/2015.
 */
var BeanClientModule = require('../beanstalk_client');
var BeanProcessorModule = require('../beanstalk_processor');
var BeanTubeModule = require('../beanstalk_tube');
var BeanJobModule = require('../beanstalk_job');
var sinon = require('sinon');
var fs = require('fs');
var moment = require('moment');
var yaml = require('js-yaml');

var assert = require('assert');

var job_exists = function(job_id){
    fs.access('../jobs/' + job_id + '.bsjob', fs.R_OK | fs.W_OK, function (err) {
        return !err;
    });
};


suite('Client', function() {
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




    suite('listTubeUsed', function () {
        test('list tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            var original_tube_count = proc.tubes.length;

            client.dataReceived("list-tube-used\r\n");

            assert(/^USING testtube\r\n$/.test(incoming_data[0]), "did not receive a good message");
        });
    });

    suite('ListTubeWatched', function () {
        test('list watched', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            client.watching.push('another');

            client.dataReceived("list-tubes-watched\r\n");

            var result = incoming_data[0].split("\r\n");

            assert(/^OK \d+$/.test(result[0]), "did not receive a reserved message");
            var stringdata = incoming_data[0].substr(7);
            var doc = yaml.safeLoad(stringdata);
            assert(doc[0] == 'testtube', "tube name is different");
            assert(doc[1] == 'another', "tube name is different");
        });
    });

    suite('ListTubes', function () {
        test('list tubes', function () {
            var proc = createProcessorWithTube('testtube');
            var tube = new BeanTubeModule.Tube();
            tube.name = 'tube1';
            proc.tubes.push(tube);
            tube = new BeanTubeModule.Tube();
            tube.name = 'tube2';
            proc.tubes.push(tube);
            var client = createClient('testtube', proc);

            client.dataReceived("list-tubes\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");

            assert(/^OK \d+$/.test(result[0]), "did not receive a reserved message");
            var stringdata = incoming_data[0].substr(7);
            var doc = yaml.safeLoad(stringdata);
            assert(doc[0] == 'testtube', "tube name is different");
            assert(doc[1] == 'tube1', "tube name is different");
            assert(doc[2] == 'tube2', "tube name is different");
        });
    });



    /*
      /$$   /$$  /$$$$$$  /$$$$$$$$
     | $$  | $$ /$$__  $$| $$_____/
     | $$  | $$| $$  \__/| $$
     | $$  | $$|  $$$$$$ | $$$$$
     | $$  | $$ \____  $$| $$__/
     | $$  | $$ /$$  \ $$| $$
     |  $$$$$$/|  $$$$$$/| $$$$$$$$
     \______/  \______/ |________/
     */
    suite('Use', function () {
        test('use non-existing tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            var original_tube_count = proc.tubes.length;

            client.dataReceived("use newtube\r\n");

            assert(/^USING newtube\r\n$/.test(incoming_data[0]), "did not receive a good message");
            assert(original_tube_count+1 == proc.tubes.length, "new tube not added");
            assert(client.tube == 'newtube', "client is not using the new tube");
        });

        test('use existing tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            var original_tube_count = proc.tubes.length;

            client.dataReceived("use testtube\r\n");

            assert(/^USING testtube\r\n$/.test(incoming_data[0]), "did not receive a good message");
            assert(original_tube_count == proc.tubes.length, "new tube added");
            assert(client.tube == 'testtube', "client is not using the right tube");
        });

        test('use bad tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("use *\r\n");

            assert(/^BAD_FORMAT\r\n$/.test(incoming_data[0]), "changed to a bad tube");
            assert(client.tube == 'testtube', "client is not using the right tube");
        });
    });



    /*
      /$$      /$$  /$$$$$$  /$$$$$$$$ /$$$$$$  /$$   /$$
     | $$  /$ | $$ /$$__  $$|__  $$__//$$__  $$| $$  | $$
     | $$ /$$$| $$| $$  \ $$   | $$  | $$  \__/| $$  | $$
     | $$/$$ $$ $$| $$$$$$$$   | $$  | $$      | $$$$$$$$
     | $$$$_  $$$$| $$__  $$   | $$  | $$      | $$__  $$
     | $$$/ \  $$$| $$  | $$   | $$  | $$    $$| $$  | $$
     | $$/   \  $$| $$  | $$   | $$  |  $$$$$$/| $$  | $$
     |__/     \__/|__/  |__/   |__/   \______/ |__/  |__/
     */
    suite('Watch', function () {
        test('watch non-existing tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            var original_tube_count = proc.tubes.length;

            client.dataReceived("watch newtube\r\n");

            assert(/^WATCHING 2\r\n$/.test(incoming_data[0]), "did not receive a good message");
            assert(original_tube_count+1 == proc.tubes.length, "new tube not added");
        });

        test('watch existing tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            var original_tube_count = proc.tubes.length;

            client.dataReceived("watch testtube\r\n");

            assert(/^WATCHING 1\r\n$/.test(incoming_data[0]), "did not receive a good message");
            assert(original_tube_count == proc.tubes.length, "new tube added");
        });

        test('watch bad tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("watch *\r\n");

            assert(/^BAD_FORMAT\r\n$/.test(incoming_data[0]), "changed to a bad tube");
        });
    });



    /*
    /$$$$$$  /$$$$$$  /$$   /$$  /$$$$$$  /$$$$$$$  /$$$$$$$$
   |_  $$_/ /$$__  $$| $$$ | $$ /$$__  $$| $$__  $$| $$_____/
     | $$  | $$  \__/| $$$$| $$| $$  \ $$| $$  \ $$| $$
     | $$  | $$ /$$$$| $$ $$ $$| $$  | $$| $$$$$$$/| $$$$$
     | $$  | $$|_  $$| $$  $$$$| $$  | $$| $$__  $$| $$__/
     | $$  | $$  \ $$| $$\  $$$| $$  | $$| $$  \ $$| $$
     /$$$$$$|  $$$$$$/| $$ \  $$|  $$$$$$/| $$  | $$| $$$$$$$$
     |______/ \______/ |__/  \__/ \______/ |__/  |__/|________/
     */
    suite('Ignore', function () {
        test('ignore non-existing tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("ignore newtube\r\n");

            assert(/^WATCHING 1\r\n$/.test(incoming_data[0]), "did not receive a good message");
        });

        test('ignore unwatched tube', function () {
            var proc = createProcessorWithTube('testtube');
            var tube = new BeanTubeModule.Tube();
            tube.name = 'anothertube';
            proc.tubes.push(tube);
            var client = createClient('testtube', proc);

            client.dataReceived("ignore anothertube\r\n");

            assert(/^WATCHING 1\r\n$/.test(incoming_data[0]), "did not receive a good message");
        });

        test('ignore last tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("ignore testtube\r\n");

            assert(/^NOT_IGNORED\r\n$/.test(incoming_data[0]), "did not receive a good message");
        });

        test('ignore bad tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("ignore *\r\n");

            assert(/^WATCHING 1\r\n$/.test(incoming_data[0]), "did not receive a good message");
        });
    });



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
    suite('Put', function () {

        test('put with new tube', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 " + data.length + "\r\n" + data + "\r\n";

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


        test('put with existing tube', function () {
            var proc = new BeanProcessorModule.BeanProcessor();

            var tube = new BeanTubeModule.Tube();
            tube.name = 'testtube';
            proc.tubes.push(tube);

            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 " + data.length + "\r\n" + data + "\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            assert.equal(1, proc.tubes.length, "more tubes created");
        });

        test('put with bad tube', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('*', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 " + data.length + "\r\n" + data + "\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            assert.equal('BAD_FORMAT\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
        });

        test('put with negative priority', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put -1 0 30 " + data.length + "\r\n" + data + "\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            assert.equal('BAD_FORMAT\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
        });

        test('put with bad length', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 1" + data.length + "\r\n" + data + "\r\n";

            client.dataReceived(data);
            proc.processCommandList();

            assert.equal('EXPECTED_CRLF\r\n', incoming_data[0]);
            assert.equal(0, proc.jobs_list.length, "created a job anyway");
        });

        test('put while draining', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            proc.drain_mode = true;
            var client = createClient('testtube', proc);
            var data = 'this is the data';
            data = "put 1000 0 30 " + data.length + "\r\n" + data + "\r\n";

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
    suite('Delete', function () {

        test('delete existing message', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var job = new BeanJobModule.BeanJob(client, 'testtube', 1000, 30, 0);
            proc.jobs_list.push(job);

            client.dataReceived("delete " + job.id + "\r\n");
            proc.processCommandList();

            assert(/^DELETED\r\n$/.test(incoming_data[0]), "did not receive a delete message");

            assert.equal(0, proc.jobs_list.length, "still exists in the list");
        });

        test('delete non-existing message', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);
            var job = new BeanJobModule.BeanJob(client, 'testtube', 1000, 30, 0);
            proc.jobs_list.push(job);

            client.dataReceived("delete 1\r\n");
            proc.processCommandList();

            assert(/^NOT_FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
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
    suite('Reserve', function () {

        test('basic reserve', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);

            client.dataReceived("reserve\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");
            assert(/^RESERVED \d+ \d+$/.test(result[0]), "did not receive a reserved message");
            assert(/^this is data$/.test(result[1]), "data text is different");

            assert(job.state == BeanJobModule.JOBSTATE_RESERVED, "not marked reserved");
        });

        test('reserve no message', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("reserve\r\n");
            proc.processCommandList();

            assert(/^TIMED_OUT\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });

        test('available outside of tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('anothertube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);

            client.dataReceived("reserve\r\n");
            proc.processCommandList();

            assert(/^TIMED_OUT\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });


    /*
      /$$$$$$$  /$$$$$$$$  /$$$$$$        /$$$$$$$$ /$$$$$$ /$$      /$$ /$$$$$$$$  /$$$$$$  /$$   /$$ /$$$$$$$$
     | $$__  $$| $$_____/ /$$__  $$      |__  $$__/|_  $$_/| $$$    /$$$| $$_____/ /$$__  $$| $$  | $$|__  $$__/
     | $$  \ $$| $$      | $$  \__/         | $$     | $$  | $$$$  /$$$$| $$      | $$  \ $$| $$  | $$   | $$
     | $$$$$$$/| $$$$$   |  $$$$$$          | $$     | $$  | $$ $$/$$ $$| $$$$$   | $$  | $$| $$  | $$   | $$
     | $$__  $$| $$__/    \____  $$         | $$     | $$  | $$  $$$| $$| $$__/   | $$  | $$| $$  | $$   | $$
     | $$  \ $$| $$       /$$  \ $$         | $$     | $$  | $$\  $ | $$| $$      | $$  | $$| $$  | $$   | $$
     | $$  | $$| $$$$$$$$|  $$$$$$/         | $$    /$$$$$$| $$ \/  | $$| $$$$$$$$|  $$$$$$/|  $$$$$$/   | $$
     |__/  |__/|________/ \______/          |__/   |______/|__/     |__/|________/ \______/  \______/    |__/
     */
    suite('Reserve Timeout', function ()
    {
        test('basic reserve', function ()
        {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);

            client.dataReceived("reserve-with-timeout 10\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");
            assert(/^RESERVED \d+ \d+$/.test(result[0]), "did not receive a reserved message");
            assert(/^this is data$/.test(result[1]), "data text is different");

            assert(job.state == BeanJobModule.JOBSTATE_RESERVED, "not marked reserved");
        });

        test('reserve after time', function ()
        {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("reserve-with-timeout 10\r\n");
            proc.processCommandList();

            // should be nothing in incoming
            assert(0 == incoming_data.length, "incoming_data has a message: " + incoming_data[0]);

            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);
            proc.checkClientReserves();

            var result = incoming_data[0].split("\r\n");
            assert(/^RESERVED \d+ \d+$/.test(result[0]), "did not receive a reserved message");
            assert(/^this is data$/.test(result[1]), "data text is different");

            assert(job.state == BeanJobModule.JOBSTATE_RESERVED, "not marked reserved");
        });


        test('reserve timeout', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("reserve-with-timeout 10\r\n");
            proc.processCommandList();

            client.reserving_until = moment().add(-1, 'seconds');
            proc.checkClientReserves();
            assert(/^TIMED_OUT\r\n$/.test(incoming_data[0]), "other than not found message");
        });

        test('reserve with zero timeout', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("reserve-with-timeout 0\r\n");
            proc.processCommandList();

            assert(/^TIMED_OUT\r\n$/.test(incoming_data[0]), "other than not found message");
        });
    });


    /*
      /$$$$$$$  /$$$$$$$$ /$$       /$$$$$$$$  /$$$$$$   /$$$$$$  /$$$$$$$$
     | $$__  $$| $$_____/| $$      | $$_____/ /$$__  $$ /$$__  $$| $$_____/
     | $$  \ $$| $$      | $$      | $$      | $$  \ $$| $$  \__/| $$
     | $$$$$$$/| $$$$$   | $$      | $$$$$   | $$$$$$$$|  $$$$$$ | $$$$$
     | $$__  $$| $$__/   | $$      | $$__/   | $$__  $$ \____  $$| $$__/
     | $$  \ $$| $$      | $$      | $$      | $$  | $$ /$$  \ $$| $$
     | $$  | $$| $$$$$$$$| $$$$$$$$| $$$$$$$$| $$  | $$|  $$$$$$/| $$$$$$$$
     |__/  |__/|________/|________/|________/|__/  |__/ \______/ |________/
     */
    suite('Release', function ()
    {
        test('immediate release', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_RESERVED);

            client.dataReceived("release "+job.id+" 1000 0\r\n");
            proc.processCommandList();

            assert(/^RELEASED\r\n$/.test(incoming_data[0]), "did not receive a released message");

            assert(job.state == BeanJobModule.JOBSTATE_READY, "not marked ready");
        });

        test('delayed release', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_RESERVED);

            client.dataReceived("release "+job.id+" 1000 30\r\n");
            proc.processCommandList();

            assert(/^RELEASED\r\n$/.test(incoming_data[0]), "did not receive a released message");

            assert(job.state == BeanJobModule.JOBSTATE_DELAYED, "not marked delayed");

            job.delay_until = moment();
            proc.checkJobTimeout();
            assert(job.state == BeanJobModule.JOBSTATE_READY, "not marked ready");
        });

        test('release not found', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("release 1 1000 0\r\n");
            proc.processCommandList();

            assert(/^NOT_FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });





    /*
      /$$$$$$$  /$$   /$$ /$$$$$$$  /$$     /$$
     | $$__  $$| $$  | $$| $$__  $$|  $$   /$$/
     | $$  \ $$| $$  | $$| $$  \ $$ \  $$ /$$/
     | $$$$$$$ | $$  | $$| $$$$$$$/  \  $$$$/
     | $$__  $$| $$  | $$| $$__  $$   \  $$/
     | $$  \ $$| $$  | $$| $$  \ $$    | $$
     | $$$$$$$/|  $$$$$$/| $$  | $$    | $$
     |_______/  \______/ |__/  |__/    |__/
     */
    suite('Bury', function () {
        test('bury existing message', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_RESERVED);

            client.dataReceived("bury " + job.id + " 1000\r\n");
            proc.processCommandList();

            assert(/^BURIED\r\n$/.test(incoming_data[0]), "did not receive a bury message");

            assert(job.state == BeanJobModule.JOBSTATE_BURIED, "not marked buried");
        });

        test('bury non-existing message', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);

            client.dataReceived("bury 1 1000\r\n");
            proc.processCommandList();

            assert(/^NOT_FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });




    /*
    /$$$$$$$$ /$$$$$$  /$$   /$$  /$$$$$$  /$$   /$$
   |__  $$__//$$__  $$| $$  | $$ /$$__  $$| $$  | $$
      | $$  | $$  \ $$| $$  | $$| $$  \__/| $$  | $$
      | $$  | $$  | $$| $$  | $$| $$      | $$$$$$$$
      | $$  | $$  | $$| $$  | $$| $$      | $$__  $$
      | $$  | $$  | $$| $$  | $$| $$    $$| $$  | $$
      | $$  |  $$$$$$/|  $$$$$$/|  $$$$$$/| $$  | $$
      |__/   \______/  \______/  \______/ |__/  |__/
     */
    suite('Touch', function () {
        test('touch existing message', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_RESERVED);

            var old_timeout = moment();

            client.dataReceived("touch " + job.id + "\r\n");
            proc.processCommandList();

            assert(/^TOUCHED\r\n$/.test(incoming_data[0]), "did not receive a touch message");

            assert(job.timeout_at.isAfter(old_timeout), "timeout not changed");
        });

        test('touch non-existing message', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);

            client.dataReceived("touch 1\r\n");
            proc.processCommandList();

            assert(/^NOT_FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });



    /*
      /$$$$$$$  /$$$$$$$$ /$$$$$$$$ /$$   /$$     /$$$$$$ /$$$$$$$
     | $$__  $$| $$_____/| $$_____/| $$  /$$/    |_  $$_/| $$__  $$
     | $$  \ $$| $$      | $$      | $$ /$$/       | $$  | $$  \ $$
     | $$$$$$$/| $$$$$   | $$$$$   | $$$$$/ /$$$$$$| $$  | $$  | $$
     | $$____/ | $$__/   | $$__/   | $$  $$|______/| $$  | $$  | $$
     | $$      | $$      | $$      | $$\  $$       | $$  | $$  | $$
     | $$      | $$$$$$$$| $$$$$$$$| $$ \  $$     /$$$$$$| $$$$$$$/
     |__/      |________/|________/|__/  \__/    |______/|_______/
     */
    suite('Peek-id', function () {
        test('peek existing message', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);

            client.dataReceived("peek "+job.id+"\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");
            assert(/^FOUND \d+ \d+$/.test(result[0]), "did not receive a message");
            assert(/^this is data$/.test(result[1]), "data text is different");
        });

        test('peek non-existing message', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);

            client.dataReceived("peek 1\r\n");
            proc.processCommandList();

            assert(/^NOT_FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });



    /*
      /$$$$$$$  /$$$$$$$$ /$$$$$$$$ /$$   /$$        /$$$$$$  /$$$$$$$$ /$$$$$$  /$$$$$$$$ /$$$$$$$$
     | $$__  $$| $$_____/| $$_____/| $$  /$$/       /$$__  $$|__  $$__//$$__  $$|__  $$__/| $$_____/
     | $$  \ $$| $$      | $$      | $$ /$$/       | $$  \__/   | $$  | $$  \ $$   | $$   | $$
     | $$$$$$$/| $$$$$   | $$$$$   | $$$$$/ /$$$$$$|  $$$$$$    | $$  | $$$$$$$$   | $$   | $$$$$
     | $$____/ | $$__/   | $$__/   | $$  $$|______/ \____  $$   | $$  | $$__  $$   | $$   | $$__/
     | $$      | $$      | $$      | $$\  $$        /$$  \ $$   | $$  | $$  | $$   | $$   | $$
     | $$      | $$$$$$$$| $$$$$$$$| $$ \  $$      |  $$$$$$/   | $$  | $$  | $$   | $$   | $$$$$$$$
     |__/      |________/|________/|__/  \__/       \______/    |__/  |__/  |__/   |__/   |________/
     */
    suite('Peek state', function () {
        test('peek ready', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);

            client.dataReceived("peek-ready\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");
            assert(/^FOUND \d+ \d+$/.test(result[0]), "did not receive a message");
            assert(/^this is data$/.test(result[1]), "data text is different");
        });

        test('peek delayed', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_DELAYED);

            client.dataReceived("peek-delayed\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");
            assert(/^FOUND \d+ \d+$/.test(result[0]), "did not receive a message");
            assert(/^this is data$/.test(result[1]), "data text is different");
        });

        test('peek buried', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_BURIED);

            client.dataReceived("peek-buried\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");
            assert(/^FOUND \d+ \d+$/.test(result[0]), "did not receive a message");
            assert(/^this is data$/.test(result[1]), "data text is different");
        });

        test('peek non-existing message', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);

            client.dataReceived("peek-buried\r\n");
            proc.processCommandList();

            assert(/^NOT_FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });



    /*
      /$$   /$$ /$$$$$$  /$$$$$$  /$$   /$$
     | $$  /$$/|_  $$_/ /$$__  $$| $$  /$$/
     | $$ /$$/   | $$  | $$  \__/| $$ /$$/
     | $$$$$/    | $$  | $$      | $$$$$/
     | $$  $$    | $$  | $$      | $$  $$
     | $$\  $$   | $$  | $$    $$| $$\  $$
     | $$ \  $$ /$$$$$$|  $$$$$$/| $$ \  $$
     |__/  \__/|______/ \______/ |__/  \__/
     */
    suite('Kick', function () {
        test('kick with buried', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            // have to set the priority so the sort is expected
            var job1 = addJob(proc, client, BeanJobModule.JOBSTATE_BURIED);
            job1.priority = 1;
            var job2 = addJob(proc, client, BeanJobModule.JOBSTATE_BURIED);
            job2.priority = 2;
            var job3 = addJob(proc, client, BeanJobModule.JOBSTATE_BURIED);
            job3.priority = 3;

            client.dataReceived("kick 2\r\n");
            proc.processCommandList();

            assert(/^KICKED 2\r\n$/.test(incoming_data[0]), "did not receive a kick");
            assert(BeanJobModule.JOBSTATE_READY==job1.state, "job not kicked");
            assert(BeanJobModule.JOBSTATE_READY==job2.state, "job not kicked");
            assert(BeanJobModule.JOBSTATE_BURIED==job3.state, "job not still buried");
        });

        test('kick with delayed', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            // have to set the priority so the sort is expected
            var job1 = addJob(proc, client, BeanJobModule.JOBSTATE_DELAYED);
            job1.priority = 1;
            var job2 = addJob(proc, client, BeanJobModule.JOBSTATE_DELAYED);
            job2.priority = 2;
            var job3 = addJob(proc, client, BeanJobModule.JOBSTATE_DELAYED);
            job3.priority = 3;

            client.dataReceived("kick 2\r\n");
            proc.processCommandList();

            assert(/^KICKED 2\r\n$/.test(incoming_data[0]), "did not receive a kick");
            assert.equal(BeanJobModule.JOBSTATE_READY, job1.state, "job not kicked");
            assert.equal(BeanJobModule.JOBSTATE_READY, job2.state, "job not kicked");
            assert.equal(BeanJobModule.JOBSTATE_DELAYED, job3.state, "job not still delayed");
        });

        test('kick with unknown tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('unknowntube', proc);

            client.dataReceived("kick 2\r\n");
            proc.processCommandList();

            assert(/^KICKED 0\r\n$/.test(incoming_data[0]), "did not receive a kick");
        });
    });



    /*
      /$$   /$$ /$$$$$$  /$$$$$$  /$$   /$$         /$$$$$  /$$$$$$  /$$$$$$$
     | $$  /$$/|_  $$_/ /$$__  $$| $$  /$$/        |__  $$ /$$__  $$| $$__  $$
     | $$ /$$/   | $$  | $$  \__/| $$ /$$/            | $$| $$  \ $$| $$  \ $$
     | $$$$$/    | $$  | $$      | $$$$$/ /$$$$$$     | $$| $$  | $$| $$$$$$$
     | $$  $$    | $$  | $$      | $$  $$|______//$$  | $$| $$  | $$| $$__  $$
     | $$\  $$   | $$  | $$    $$| $$\  $$      | $$  | $$| $$  | $$| $$  \ $$
     | $$ \  $$ /$$$$$$|  $$$$$$/| $$ \  $$     |  $$$$$$/|  $$$$$$/| $$$$$$$/
     |__/  \__/|______/ \______/ |__/  \__/      \______/  \______/ |_______/
     */
    suite('Kick-job', function () {
        test('kick delayed job', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_DELAYED);

            client.dataReceived("kick-job "+job.id+"\r\n");
            proc.processCommandList();

            assert(/^KICKED\r\n$/.test(incoming_data[0]), "did not receive a kick");
            assert.equal(BeanJobModule.JOBSTATE_READY, job.state, "job not kicked");
        });

        test('kick buried job', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_BURIED);

            client.dataReceived("kick-job "+job.id+"\r\n");
            proc.processCommandList();

            assert(/^KICKED\r\n$/.test(incoming_data[0]), "did not receive a kick");
            assert.equal(BeanJobModule.JOBSTATE_READY, job.state, "job not kicked");
        });

        test('kick ready job', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);

            client.dataReceived("kick-job "+job.id+"\r\n");
            proc.processCommandList();

            assert(/^NOT_FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });

        test('kick non-existing job', function () {
            var proc = new BeanProcessorModule.BeanProcessor();
            var client = createClient('testtube', proc);

            client.dataReceived("kick-job 1\r\n");
            proc.processCommandList();

            assert(/^NOT_FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });


    /*
      /$$$$$$$   /$$$$$$  /$$   /$$  /$$$$$$  /$$$$$$$$
     | $$__  $$ /$$__  $$| $$  | $$ /$$__  $$| $$_____/
     | $$  \ $$| $$  \ $$| $$  | $$| $$  \__/| $$
     | $$$$$$$/| $$$$$$$$| $$  | $$|  $$$$$$ | $$$$$
     | $$____/ | $$__  $$| $$  | $$ \____  $$| $$__/
     | $$      | $$  | $$| $$  | $$ /$$  \ $$| $$
     | $$      | $$  | $$|  $$$$$$/|  $$$$$$/| $$$$$$$$
     |__/      |__/  |__/ \______/  \______/ |________/
     */
    suite('Pause', function () {
        test('pause with jobs', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);

            client.dataReceived("pause-tube testtube 60\r\n");
            proc.processCommandList();

            assert(/^PAUSED\r\n$/.test(incoming_data[0]), "did not receive a kick");

            var joblocate = proc.getJobForClient(client, BeanJobModule.JOBSTATE_READY);
            assert(joblocate == null, "was able to find a job in a paused tube");
        });

        test('unpause with jobs', function () {
            var proc = createProcessorWithTube('testtube');
            proc.tubes[0].pause_until = moment().add(60, 'seconds');
            proc.tubes[0].paused = true;
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);

            // baseline, we should have a paused tube
            var joblocate = proc.getJobForClient(client, BeanJobModule.JOBSTATE_READY);
            assert(joblocate == null, "was able to find a job in a paused tube");

            // set the time to unpause to now
            proc.tubes[0].pause_until = moment().add(-1, 'seconds');
            proc.checkTubesPaused();

            // now it should be available
            joblocate = proc.getJobForClient(client, BeanJobModule.JOBSTATE_READY);
            assert(joblocate != null, "was not able to find a job");
        });

        test('pause unknown tube', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("pause-tube unknowntube 30\r\n");
            proc.processCommandList();

            assert(/^NOT_FOUND\r\n$/.test(incoming_data[0]), "did not receive a not found message");
        });
    });

    /*
      /$$$$$$  /$$$$$$$$ /$$$$$$  /$$$$$$$$ /$$$$$$
     /$$__  $$|__  $$__//$$__  $$|__  $$__//$$__  $$
     | $$  \__/   | $$  | $$  \ $$   | $$  | $$  \__/
     |  $$$$$$    | $$  | $$$$$$$$   | $$  |  $$$$$$
     \____  $$   | $$  | $$__  $$   | $$   \____  $$
     /$$  \ $$   | $$  | $$  | $$   | $$   /$$  \ $$
     |  $$$$$$/   | $$  | $$  | $$   | $$  |  $$$$$$/
     \______/    |__/  |__/  |__/   |__/   \______/
     */
    suite.only('Stats', function () {
        test('job stats', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);
            var job = addJob(proc, client, BeanJobModule.JOBSTATE_READY);

            client.dataReceived("stats-job "+job.id+"\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");

            assert(/^OK \d+$/.test(result[0]), "did not receive a good message");
            var stringdata = incoming_data[0].substr(8);
            var doc = yaml.safeLoad(stringdata);
            assert(doc.state == "ready");
        });

        test('tube stats', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("stats-tube testtube\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");

            assert(/^OK \d+$/.test(result[0]), "did not receive a good message");
        });

        test('server stats', function () {
            var proc = createProcessorWithTube('testtube');
            var client = createClient('testtube', proc);

            client.dataReceived("stats\r\n");
            proc.processCommandList();

            var result = incoming_data[0].split("\r\n");

            assert(/^OK \d+$/.test(result[0]), "did not receive a good message");
        });
    });

});
