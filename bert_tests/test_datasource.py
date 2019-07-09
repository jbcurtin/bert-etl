
def test_psql_context():
  from bert import datasource, utils
  with datasource.Postgres.ParseURL('postgres://jbcurtin:@localhost:5432/jbcurtin'):
    cmd: str = 'psql --list'
    result = utils.run_command(cmd)
    assert 'jbcurtin' in result

