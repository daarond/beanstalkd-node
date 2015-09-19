/**
 * Created by daarond on 9/18/2015.
 */

var _ = require("underscore");
var net = require('net');
var assert = require('assert');
var fivebeans = require('fivebeans');
var BeanProcessorModule = require('../beanstalk_processor');

suite('API', function() {
    var client = null;

    before(function () {
        // start up the server
        var proc = new BeanProcessorModule.BeanProcessor();
        proc.start(8003);

        client = new fivebeans.client('127.0.0.1', 8003);
        client
            .on('connect', function()
            {
                // client can now be used
                console.log('Connected\n');
            })
            .on('error', function(err)
            {
                // connection failure
                console.log('ERROR:'+err+'\n');
            })
            .on('close', function()
            {
                // underlying connection has closed
                console.log('Closed\n');
            })
            .connect();
    });

    test('watch tube', function () {
        client.watch('testtube', function(err, numwatched) {
            client.list_tubes_watched(function(err, tubelist) {
                assert(tubelist.length==1, 'tube not matching');
                assert(tubelist[0] == 'testtube', 'name does not match');
            });
        });
    });

    test('ignore tube', function () {
        client.watch('testtube1', function (err, numwatched) {
            client.watch('testtube2', function (err, numwatched) {
                client.list_tubes_watched(function (err, tubelist) {
                    assert(tubelist.length == 2, 'tube not matching');
                    client.ignore('testtube2', function (err, numwatched) {
                        assert(numwatched==1, 'count not matching');
                    });
                });
            });
        });
    });

    test('put tube', function () {
        client.put(1000, 0, 30, 'example data here', function(err, jobid) {
            client.peek(jobid, function(err, jobid, payload) {
                assert(payload == 'example data here');
                client.destroy(jobid);
            });
        });
    });

    test('reserve tube', function () {
        client.put(1000, 0, 30, 'example data here', function(err, jobid) {
            client.reserve(function(err, jobid, payload) {
                assert(payload == 'example data here');
                client.stats_job(jobid, function(err, response) {
                    console.log(response);
                });
                client.destroy(jobid);
            });
        });
    });
});
