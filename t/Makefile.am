AM_CFLAGS = \
	$(WARNING_CFLAGS) \
	$(CODE_COVERAGE_CFLAGS)

AM_LDFLAGS = \
	$(CODE_COVERAGE_LIBS)

AM_CPPFLAGS = \
	-I$(top_srcdir) \
	-I$(top_srcdir)/src/common/libutil \
	-I$(top_srcdir)/src/common/select \
	$(FLUX_CORE_CFLAGS) \
	$(JANSSON_CFLAGS)

TEST_EXTENSIONS = .t
T_LOG_DRIVER = env AM_TAP_AWK='$(AWK)' $(SHELL) \
	$(top_srcdir)/config/tap-driver.sh

TESTSCRIPTS = \
	t0000-sharness.t \
	t1000-delegate-load.t \
	t1001-delegate-assign.t \
	t1002-delegate-cancel.t

# dist_check_SCRIPTS are built and installed for 'make distcheck'
dist_check_SCRIPTS = \
	$(TESTSCRIPTS) \
	sharness.sh \
	aggregate-results.sh \
	runtests.sh

# Additional files to distribute
EXTRA_DIST = \
	sharness.d \
	scripts

# Set TESTS to the test scripts to run
TESTS = $(TESTSCRIPTS)

# Environment for running tests
AM_TESTS_ENVIRONMENT = \
	export FLUX_PREFIX="${FLUX_PREFIX}";\
	export SHARNESS_TEST_SRCDIR="$(abs_srcdir)";\
	export SHARNESS_BUILD_DIRECTORY="$(abs_top_builddir)";\
	export PATH="$(abs_top_builddir)/src:$$PATH";

clean-local:
	rm -rf trash-directory.* test-results .prove *.broker.log *.output

# Allow 'make check' to work
check-local: $(TESTSCRIPTS)
	@echo "Running sharness tests..."
