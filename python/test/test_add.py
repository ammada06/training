from src.add.add import add
 
def test_add():
    assert add(2, 3) == 5
    assert add(2, 2) != 5
 