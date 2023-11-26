import fusion
from fusion.libs.entity import entity_type, Entity
from fusion.libs.channel import Channel
from fusion.loop import NoMainLoop


def test_pubsub():
    main_loop = NoMainLoop()
    fusion.set_main_loop(main_loop)

    channel = Channel('test_channel',
                      index_key=lambda x: x.id,
                      filter_key=lambda x: x.text != 'NO')

    @entity_type
    class MockNote(Entity):
        text: str = ''

    expected_notes = []
    all_notes = []
    for text in ['Yes', 'Blah', 'NO']:
        note = MockNote(text=text)
        all_notes.append(note)
        if text != 'NO':
            expected_notes.append(note)

    expected_on_the_indexed_channel = expected_notes[0]

    received_notes = []
    received_with_index_val_set = []

    def handle_all(message):
        received_notes.append(message)

    def handle_indexed_channel(message):
        received_with_index_val_set.append(message)

    channel.subscribe(handle_all)
    channel.subscribe(handle_indexed_channel,
                      index_val=expected_on_the_indexed_channel.id)

    for note in all_notes:
        channel.push(note)
    main_loop.process_events()

    assert received_notes == expected_notes
    assert received_with_index_val_set == [expected_on_the_indexed_channel]
