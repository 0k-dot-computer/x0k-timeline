use serde::{Deserialize, Serialize};
use x0k_serialization::{serialize, PostcardSerializable};

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, PostcardSerializable)]
struct TestStruct {
    doc_type_id: u64,
    title: String,
}

#[test]
fn test_large_u64_serialization() {
    // Test with large u64 value like Observatory's
    let large_id: u64 = 18135277348960862209;

    println!(
        "Testing postcard serialization with large u64: {}",
        large_id
    );
    println!("As hex: 0x{:016x}", large_id);

    // Direct struct creation and serialization
    let test_struct = TestStruct {
        doc_type_id: large_id,
        title: "Test".to_string(),
    };

    // Serialize
    let postcard_bytes = serialize::to_bytes(&test_struct).unwrap();
    println!("Postcard bytes: {} bytes", postcard_bytes.len());

    // Deserialize back
    let back: TestStruct = serialize::from_bytes(&postcard_bytes).unwrap();
    assert_eq!(back, test_struct);
    assert_eq!(back.doc_type_id, large_id);
    println!("Successfully round-tripped large u64!");
}

#[test]
fn test_direct_struct_serialization() {
    let large_id: u64 = 18135277348960862209;

    // Direct struct creation and serialization
    let test_struct = TestStruct {
        doc_type_id: large_id,
        title: "Test".to_string(),
    };

    // Serialize directly
    let postcard_bytes = serialize::to_bytes(&test_struct).unwrap();
    println!("Direct postcard bytes: {} bytes", postcard_bytes.len());

    // Deserialize back
    let back: TestStruct = serialize::from_bytes(&postcard_bytes).unwrap();
    assert_eq!(back, test_struct);
}
