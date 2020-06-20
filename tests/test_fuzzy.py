from . import fuzzydict, fuzzylist


def test_fuzzylist():
    x = fuzzylist('a', 'b', 'c', maybe=('x', 'y', 'z'), max_maybe_items={'x':1})
    assert     x != ['a', 'b']
    assert not x == ['a', 'b']
    assert     x == ['a', 'c', 'b']
    assert not x != ['a', 'c', 'b']
    assert     x == ['a', 'x', 'c', 'y', 'b']
    assert not x != ['a', 'x', 'c', 'y', 'b']
    assert     x == ['a', 'x', 'b', 'z', 'c', 'y']
    assert not x != ['a', 'x', 'b', 'z', 'c', 'y']
    assert     x != ['a', 'l', 'b', 'z', 'c', 'y']
    assert not x == ['a', 'l', 'b', 'z', 'c', 'y']
    assert     x != ['x', 'b', 'x', 'a', 'c', 'y']
    assert not x == ['x', 'b', 'x', 'a', 'c', 'y']
    assert fuzzylist(0) == fuzzylist(maybe=(0,))
    assert fuzzylist(maybe=(0,)) == fuzzylist(0)
    assert fuzzylist(0) != fuzzylist(maybe=(1,))
    assert fuzzylist(maybe=(1,)) != fuzzylist(0)
    assert [1, 1, 2, 3] != fuzzylist(1, 2, 3)
    assert fuzzylist(1, 2, 3) != [1, 1, 2, 3]
    assert fuzzylist(0, 0, 1) == fuzzylist(0, 1, maybe=[0])
    assert fuzzylist(0, 1, maybe=[0]) == fuzzylist(0, 0, 1)

def test_fuzzydict():
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) == {'a': 'foo'}
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) == {'a': 'foo', 'b': []}
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) != {'a': 'foo', 'b': ['bar']}
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) != {'b': []}
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) != {}
    assert fuzzydict(b=fuzzylist(maybe=(1, 2, 3))) == {}
    assert fuzzydict(b=fuzzylist(maybe=(1, 2, 3))) == {'x': fuzzylist(maybe=(4, 5, 6))}
