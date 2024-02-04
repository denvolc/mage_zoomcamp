if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def column_rename(data, *args, **kwargs):
    return data.rename(columns={
        'VendorID':'vendor_id', 
        'RatecodeID':'ratecode_id',
        'PULocationID':'pu_location_id',
        'DOLocationID':'do_location_id'})

def add_date_columns(data, *args, **kwargs):
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data['lpep_dropoff_date'] = data['lpep_dropoff_datetime'].dt.date
    return data

def remove_zero_values(data, *args, **kwargs):
    print(f"Preprocessing: rows with zero passengers: {(data['passenger_count']==0).sum()}")
    data = data[data['passenger_count'] > 0]
    print(f"Preprocessing: rows with zero distance: {(data['trip_distance']==0).sum()}")
    data = data[data['trip_distance'] > 0]
    return data

@transformer
def transformer(data, *args, **kwargs):
    data = column_rename(data)
    data = add_date_columns(data)
    data = remove_zero_values(data)
    return data

@test
def test_columns_renamed(data, *args):
    assert 'vendor_id' in list(data), 'Column rename failed'

@test
def test_zero_passengers(data, *args):
    assert (data['passenger_count']==0).sum() == 0, 'There are rides with zero passengers'

@test
def test_zero_trip_distance(data, *args):
    assert (data['trip_distance']==0).sum() == 0, 'There are rides with zero distance'

@test
def test_output(data, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert data is not None, 'The output is undefined'
