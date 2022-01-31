const c_tab = {
  0: 1,
  1: 7,
  2: 4.5,
  3: 3,
  4: 2,
  5: 1.5,
};

const f_tab = {
  A: 5,
  B: 5,
  C: 5,
  D: 5,
  E: 2.5,
  F: 1,
};

let pos_tab = {
  0: [0, 0],
  1: [5, 10],
  2: [4.5, 9],
  3: [4, 8],
  4: [3.5, 7],
  5: [3, 6],
  6: [3, 6],
  7: [3, 6],
  8: [3, 6],
  9: [3.5, 7],
  10: [4, 8],
  11: [4, 8],
  12: [4.5, 9],
};

module.exports.calc_race_score = ({ rc, fee_tag, position, flame }) => {
  let c_sc = c_tab[rc] || 0;
  let f_sc = f_tab[fee_tag] || 0;
  let p_sc = pos_tab[position][flame] || 0;
  if ([5, 6, 7, 8].includes(parseInt(position)) && flame == 0) f_sc = f_sc / 2;
  let sc = c_sc + f_sc + p_sc;
  return sc;
};

module.exports.calc_race_score_det = ({ rc, fee_tag, position, flame }) => {
  let c_sc = c_tab[rc] || 0;
  let f_sc = f_tab[fee_tag] || 0;
  let p_sc = pos_tab[position][flame] || 0;
  if ([5, 6, 7, 8].includes(parseInt(position)) && flame == 0) f_sc = f_sc / 2;
  let sc = c_sc + f_sc + p_sc;
  return {
    c_sc,
    f_sc,
    p_sc,
    sc,
  };
  // return sc;
};
