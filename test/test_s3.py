from warcio.utils import open_or_default
from warcio.cli import main

from . import get_test_file
from .test_cli import check_helper
from .conftest import requires_aws_s3


@requires_aws_s3
def test_recompress_warc_verbose(capsys, s3_tmpdir):
    compress_output_path = s3_tmpdir + "/foo.warc.gz"

    test_file = get_test_file('example.warc.gz')

    # recompress!
    main(args=['recompress', '-v', test_file, compress_output_path])

    out = capsys.readouterr().out
    assert '{"offset": "0", "warc-type": "warcinfo"}' in out
    assert '"warc-target-uri": "http://example.com/"' in out

    assert 'No Errors Found!' in out
    assert '6 records read' in out


@requires_aws_s3
def test_write_and_read_s3(s3_tmpdir):
    file_path = s3_tmpdir + "/foo.text"

    with open_or_default(file_path, "wt") as f:
        f.write("foo")

    with open_or_default(file_path, "rt") as f:
        content = f.read()

    assert content == "foo", "invalid file content"


@requires_aws_s3
def test_copy_to_s3_and_check_extract(s3_tmpdir, capsys):
    input_file = get_test_file('example.warc.gz')
    output_file = s3_tmpdir + '/example.warc.gz'

    # copy text file to S3
    with open(input_file, "rb") as input_f:
        with open_or_default(output_file, "wb") as output_f:
            output_f.write(input_f.read())

    # check uploaded file
    check_output = check_helper(['check', '-v', output_file], capsys, 0)
    assert 'Invalid' not in check_output

    # extract from uploaded file
    extract_output = check_helper(['extract', output_file, '0'], capsys, None)
    assert 'WARC-Filename: temp-20170306040353.warc.gz' in extract_output
