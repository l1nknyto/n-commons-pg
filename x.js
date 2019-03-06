var xxx = function(counter) {
  console.log('counter', counter);
  if (counter) xxx(counter - 1);
};

xxx(100);