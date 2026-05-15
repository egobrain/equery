.PHONY: all compile test check dialyzer xref eunit docs clean \
        release-patch release-minor release-major

REBAR3 ?= rebar3

all: compile

compile:
	$(REBAR3) compile

eunit:
	$(REBAR3) eunit

dialyzer:
	$(REBAR3) dialyzer

xref:
	$(REBAR3) xref

test: eunit

check: eunit dialyzer xref

docs:
	$(REBAR3) ex_doc

clean:
	$(REBAR3) clean
	rm -rf doc/

##
## Release helpers: bump version in CHANGELOG, commit, tag.
## After successful run, push tags and publish:
##
##   git push --follow-tags
##   rebar3 hex publish
##

release-patch:
	@./scripts/release.sh patch

release-minor:
	@./scripts/release.sh minor

release-major:
	@./scripts/release.sh major
