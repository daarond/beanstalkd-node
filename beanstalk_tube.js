/**
 * Created by daarond on 9/9/2015.
 */

var Tube = function()
{
    var self = this;
    self.name = '';
    self.tube_stats = [];
    self.paused = false;
    self.pause_start = 0;
    self.pause_until = moment().add(delay_seconds, 'seconds');
    self.total_jobs = 0;
    self.total_deletes = 0;
    self.total_pauses = 0;
};

exports.Tube = Tube;
