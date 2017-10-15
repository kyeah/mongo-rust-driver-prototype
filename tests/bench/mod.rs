#![feature(test)]
use test::Bencher;
use mongodb::Client;

#[bench]
fn trivial_query(b: &mut Bencher) {
    let client = Client::connect("localhost", 27017).unwrap();
    let coll = client.db("test").collection("movies");

    let doc = doc! { "title" => "Jaws",
                      "array" => [ 1, 2, 3 ] };

    // Insert document into 'test.movies' collection
    coll.insert_one(doc.clone())
        .ok().expect("Failed to insert document.");

    b.iter(|| {
        let mut cursor = coll.find(Some(doc.clone()))
            .ok().expect("Failed to execute find.");
    })
}
