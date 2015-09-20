# beanstalkd-node
Beanstalkd server implemented in node.js. I wrote this to have a working beanstalkd server in windows.
This is unfit for production or mission-critical operations.

Let me repeat: <strong>using this in production is a terrible idea</strong>.

## Installing
You need to have an install of node.js and npm. Download the tar/zip of this package.

Once downloaded, you need to install the dependencies for the server with npm.
```
npm install
```

If you want to play with the web server, you'll need to install the dependencies with bower.
```
bower install
```

Now that all the pieces are in place, you can start up the beanstalk server with grunt with a simple:
```
grunt
```

## CAVEAT PROGRAMMER
While it does respond to beanstalk protocol, all jobs are only in memory-- it does not keep data on a restart.
If people become interested in it, I will implement file serialization.
