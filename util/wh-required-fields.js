var moment = require('moment');

module.exports = whRequiredDates;

function formattedDate(date) {
  return moment(date).format();
}

function guid () {
  return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
         s4() + '-' + s4() + s4() + s4();
}

function s4 () {
  return Math.floor((1 + Math.random()) * 0x10000)
             .toString(16)
             .substring(1);
}

function whRequiredDates (d) {
  var time = new Date();

  if ( ! d.hasOwnProperty( 'create_date' ) ) d.create_date = formattedDate(time);
  d.publish_date = formattedDate(time);
  d.last_updated = formattedDate(time);
  if ( ! d.hasOwnProperty( '_sort_create_date' ) ) d._sort_create_date = time.getTime();
  d._sort_last_updated = time.getTime();
  d._sort_publish_date = time.getTime();
  if ( ! d.hasOwnProperty( 'preview_url' ) ) d.preview_url = guid();

  return d;
}
