import DI_Tools as DI
import datetime as dt

def test_DI_Tools_exists():
    DI.readme()
    print('\n')

#def test_can_find_s3_objects():
#  start_time = dt.datetime(2022, 7, 5, 3, 4, 0, tzinfo = dt.timezone.utc)
#  end_time = dt.datetime(2022, 7, 5, 3, 5, 0, tzinfo = dt.timezone.utc)
#    num = len(DI.query_APC_s3_by_LastMod_time(start_time, end_time, 'can-001-1-03-ap-northeast-1-staging', download='N'))
#    assert num > 0

def test_basic_DI_compare():
    ground_truth = DI.DataIntegrity(DI.log_to_object('ground_truth.log', 0, 2), name='ground_truth', sampled='N')
    returned_data = DI.DataIntegrity(DI.csv_to_object('latest.csv', 5, 6), name='tile', sampled='N')
    results = ground_truth.compare(returned_data)
    print('\n')
    assert results[0] > 0.99
    assert results[1] < 1.00
    print(f'The Match Score is: {round(results[0], 4)}')
    print(f'The Maximum Delta is: {round(results[1], 4)}')
    print('Test Passed.')