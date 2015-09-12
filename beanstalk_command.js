/**
 * Created by daarond on 9/9/2015.
 */

var BeanCommand = function(client, tube)
{
    this.command_type = 0;
    this.commandline = [];
    this.data = '';
    this.client = null;
    this.tube = '';
};

exports.BeanCommand = BeanCommand;
