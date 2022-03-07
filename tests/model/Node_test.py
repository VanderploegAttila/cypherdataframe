def test_return_properties(plant_node):
    assert (['Plant.value',
             'Plant.createdOn'] == plant_node.return_properties())
