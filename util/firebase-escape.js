module.exports = firebaseEscape;

function firebaseEscape ( site ) {
  return site.toLowerCase().replace( /\./g, ',1' )
}
