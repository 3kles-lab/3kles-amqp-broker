var glob = require('glob');
var babelify = require('babelify');
var gulp = require('gulp');
var useTsConfig = require('gulp-use-tsconfig');
var clean = require('gulp-clean');
const terser = require('gulp-terser');
var sourcemaps = require('gulp-sourcemaps');
var source = require('vinyl-source-stream');
var buffer = require('vinyl-buffer');
var tap = require('gulp-tap');
var concat = require('gulp-concat');
var rename = require('gulp-rename');
var browserify = require('browserify');
var gutil = require('gulp-util');
var runSequence = require('run-sequence');
var exec = require('child_process').exec;
var tsConfig = './tsconfig.json';
var es = require('event-stream');


// INIT VARIABLES
const DIST_DIR = 'dist';
const BUILD_DIR = 'build';
const ALL_JS = '/**/*.js';
var options = {
	continueOnError: false, // default = false, true means don't emit error event
	pipeStdout: false, // default = false, true means stdout is written to file.contents
	customTemplatingThing: "test" // content passed to lodash.template()
};
var reportOptions = {
	err: true, // default = true, false means don't write err
	stderr: true, // default = true, false means don't write stderr
	stdout: true // default = true, false means don't write stdout
};



// BUILD
gulp.task('lint', () => {
	return gulp.src(tsConfig)
		.pipe(useTsConfig.lint());
});

gulp.task('pre-build', () => {
	return gulp.src(tsConfig)
		.pipe(useTsConfig.clean()); // Remove all .js; .map and .d.ts files
});

gulp.task('transpile', gulp.series('lint', 'pre-build', () => {
	return gulp.src(tsConfig)
		.pipe(useTsConfig.build());// generates .js and optionaly .map anod/or .d.ts files
}));



gulp.task('build-js', (done) => {
	var files = glob.sync('./' + DIST_DIR + ALL_JS);

	var tasks = files.map(function(file) {
        return browserify({ entries: [file] })
			.transform(babelify, { presets: ['env'] })
            .bundle()
            .pipe(source(file))
			.pipe(rename({ extname: '.min.js' }))
            .pipe(buffer())
			.pipe(sourcemaps.init({ loadMaps: true }))
			.pipe(terser())
			.on('error', function (err) { gutil.log(gutil.colors.red('[Error]'), err.toString()); })
			.pipe(sourcemaps.write('./'))
			.pipe(gulp.dest(BUILD_DIR))
        });
    return es.merge.apply(null, tasks).on('end',done);
});



gulp.task('js', function () {
	return gulp.src(DIST_DIR + ALL_JS, { read: false }) // no need of reading file because browserify does.
		// transform file objects using gulp-tap plugin
		.pipe(tap(function (file) {

			// replace file contents with browserify's bundle stream
			file.contents = browserify(file.path, { debug: true }).bundle();

		}))

		// transform streaming contents into buffer contents (because gulp-sourcemaps does not support streaming contents)
		.pipe(buffer())

		// load and init sourcemaps
		.pipe(sourcemaps.init({ loadMaps: true }))

		.pipe(terser())

		// write sourcemaps
		.pipe(sourcemaps.write('./'))

		.pipe(gulp.dest('dest'));

});

// CLEAN
gulp.task('clean-module', () => {
	return gulp.src('node_modules', { force: true })
		.pipe(clean());
});

gulp.task('clean-dist', (done) => {
	return gulp.src(DIST_DIR, { force: true })
		.on('error', () => {
			console.log('Dist does not exist')
			done()
		})
		.pipe(clean())

});

gulp.task('clean-build', (done) => {
	return gulp.src(BUILD_DIR, { force: true })
		.on('error', () => {
			console.log('Build does not exist')
			done()
		})
		.pipe(clean());
});

// PUBLISH
gulp.task('publish', gulp.series('transpile', (cb) => {
	exec('npm publish --force', function (err, stdout, stderr) {
		console.log(stdout);
		console.log(stderr);
		cb(err);
	});
}));


// WATCH
gulp.task('watch', gulp.series('transpile'), () => {
	return gulp.src(tsConfig)
		.pipe(useTsConfig.watch());
});


// PROD
gulp.task('build-prod', gulp.series('clean-dist', 'clean-build', 'transpile', 'build-js', (done) => {
	done();
}));

