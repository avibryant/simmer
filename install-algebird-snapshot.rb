#!/usr/bin/env ruby

Dir["../algebird/algebird-core/target/scala-2.9.2/*"].each do |file|
	if file =~ /algebird-core_2.9.2-(.*-SNAPSHOT).(jar|pom)$/
		system("mvn install:install-file -Dfile=#{file} -DgroupId=com.twitter -DartifactId=algebird-core_2.9.2 -Dversion=#{$1} -Dpackaging=#{$2}")
	end
end