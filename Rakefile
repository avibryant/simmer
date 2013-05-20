def find_scalac
  scalac = ["zinc", "scalac"].map{|c| `which #{c}`}.detect{|c| c != ""}
  unless scalac
    $stderr.puts "Could not find a Scala compiler"
    exit(1)
  end
  scalac.chomp!
  scalac += " -nailed" if(scalac =~ /zinc/)
  scalac
end

file "default" => ["build"]

file "build" => [".classpath"] do |t|
  Dir.mkdir("target") unless Dir.exists?("target")
  Dir.mkdir("target/classes") unless Dir.exists?("target/classes")

  classpath = File.read(".classpath")
  sources = Dir["src/**/*.scala"]
  sh "#{find_scalac} -d target/classes -cp #{classpath} #{sources.join(' ')}"
end

file ".classpath" => ["pom.xml"]  do |t|
  sh "rm -f .classpath && mvn -q dependency:build-classpath -Dmdep.outputFile=.classpath"
end