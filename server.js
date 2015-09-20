/*************************************
//
// beanstalknoded app
//
**************************************/

// express magic
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io').listen(server);
var device  = require('express-device');
var _ = require("underscore");
var BeanProcessorModule = require('./beanstalk_processor');

var beanstalkPortNumber = 11300;
var runningPortNumber = 8002;

// I need to access everything in '/public' directly
app.use(express.static(__dirname + '/public'));

//set the view engine
app.set('view engine', 'ejs');
app.set('views', __dirname +'/views');

app.use(device.capture());


// logs every request
app.use(function(req, res, next){
	// output every request in the array
	console.log({method:req.method, url: req.url, device: req.device});

	// goes onto the next function in line
	next();
});

app.get("/", function(req, res){
	res.render('index', {});
});

app.get("/clients", function(req, res){
    var json = proc.getClientsJson();
    res.send(json);
});

app.get("/jobs", function(req, res){
    var json = proc.getJobsJson();
    res.send(json);
});

var proc = new BeanProcessorModule.BeanProcessor();
proc.start(beanstalkPortNumber);

server.listen(runningPortNumber);

