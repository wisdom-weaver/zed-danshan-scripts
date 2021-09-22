await init();
  let doc = await zed_db.db.collection("horse_details").findOne({ hid: 60503 });
  console.log(doc);