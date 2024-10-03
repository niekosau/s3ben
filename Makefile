default: run

run:
	poetry lock
	poetry install
	poetry run s3ben --log-level debug --config etc/s3ben.conf $(args)

clean:
	for b in `radosgw-admin bucket list | jq -r .[]`; do radosgw-admin notification rm --bucket $$b; done
	radosgw-admin topic rm --topic s3ben-exchange

publish:
	poetry build
	poetry publish

test:
	poetry run s3ben --log-level debug --config etc/s3ben.conf test
