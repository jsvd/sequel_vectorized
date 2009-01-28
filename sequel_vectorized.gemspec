spec = Gem::Specification.new do |s|
	s.name              = "sequel_vectorized"
	s.version           = "0.0.2"
	s.platform          = Gem::Platform::RUBY
	s.has_rdoc          = false
	s.summary           = ""
	s.description       = ""
	s.author            = "João Duarte"
	s.email             = "jsvduarte@gmail.com"
	#s.executables       = %w(  )
	#s.bindir            = "bin"
	s.require_path      = "lib"

	s.add_dependency('sequel', '>=2.9.0')
	s.add_dependency('narray', '>=0.5.8')
	s.required_ruby_version = '>= 1.8.5'

  s.files = %w(
    lib/sequel_vectorized.rb
    README.rdoc
    spec/sequel_vectorized_spec.rb
    sequel_vectorized.gemspec
    ChangeLog)

end
