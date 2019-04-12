var operator = '=ANY($)';
var result = createCondition('id', '123', operator);
console.log('result', result);


function createCondition(field, value, operator) {
  var varIndex = '$' + 2;
  if (operator.indexOf('$') != -1) {
    return field + operator.replace("$", varIndex);
  } else {
    return field + operator + '$' + this.params.length;
  }
}