use bot_framework::arguments::{ArgumentError, Arguments, SlashValue};

#[test]
fn both_sources_extract_the_same_types() {
    let mut prefix = Arguments::prefix("hello -12 true").unwrap();
    let mut slash = Arguments::slash([
        ("flag".into(), SlashValue::Boolean(true)),
        ("number".into(), SlashValue::Integer(-12)),
        ("text".into(), SlashValue::String("hello".into())),
    ])
    .unwrap();
    for args in [&mut prefix, &mut slash] {
        assert_eq!(args.required::<String>("text").unwrap(), "hello");
        assert_eq!(args.required::<i64>("number").unwrap(), -12);
        assert!(args.required::<bool>("flag").unwrap());
        assert_eq!(args.optional::<String>("optional").unwrap(), None);
    }
    prefix.finish().unwrap();
    slash.finish().unwrap();
}
#[test]
fn invalid_and_unknown_inputs_do_not_silently_coerce() {
    assert!(matches!(
        Arguments::prefix("'two words'"),
        Err(ArgumentError::UnsupportedPrefixSyntax)
    ));
    let mut args = Arguments::prefix("12x extra").unwrap();
    assert!(matches!(
        args.required::<i64>("count"),
        Err(ArgumentError::Invalid { .. })
    ));
    assert!(matches!(args.finish(), Err(ArgumentError::Unexpected(_))));
    let mut args = Arguments::slash([("count".into(), SlashValue::String("12".into()))]).unwrap();
    assert!(matches!(
        args.required::<i64>("count"),
        Err(ArgumentError::Invalid { .. })
    ));
    assert!(matches!(
        args.required::<bool>("missing"),
        Err(ArgumentError::Missing(_))
    ));
    assert!(matches!(
        Arguments::slash([
            ("x".into(), SlashValue::Boolean(true)),
            ("x".into(), SlashValue::Boolean(false))
        ]),
        Err(ArgumentError::Duplicate(_))
    ));
    let args = Arguments::slash([("extra".into(), SlashValue::Integer(1))]).unwrap();
    assert_eq!(
        args.finish(),
        Err(ArgumentError::Unexpected("extra".into()))
    );
}
