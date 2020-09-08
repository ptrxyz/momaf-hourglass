require 'active_record'

ActiveRecord::Base.establish_connection(adapter: 'sqlite3', database: 'test.db')

# SQLite3 statements to create needed tables for example:

=begin
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT
);
CREATE TABLE profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INT,
    bio TEXT
);
CREATE TABLE posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INT,
    content TEXT
);
CREATE TABLE clans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT
);
CREATE TABLE clans_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INT,
    clan_id INT
);
=end

class User < ActiveRecord::Base
  has_one :profile
  has_many :posts
  has_and_belongs_to_many :clans

  before_create :before_create_callback
  after_create :after_create_callback

  def before_create_callback
    puts "About to create user: #{self.name}"
  end

  def after_create_callback
    puts "New user object created: #{self.name}"
  end
  
end

class Profile < ActiveRecord::Base
  belongs_to :user
end

class Post < ActiveRecord::Base
  belongs_to :user

  before_destroy :before_destroy_callback
  after_destroy :after_destroy_callback

  def before_destroy_callback
    puts "About to destroy post: #{self.id}"
  end

  def after_destroy_callback
    puts "Post destroyed: #{self.id}"
  end
  
end

class Clan < ActiveRecord::Base
  has_and_belongs_to_many :users
end


dano = User.create(name: 'NanoDano')

# A few ways to create the profile for the user
# Since this is a one-to-one relationship,
# only one will actually be tied to user,
# and the others will end up with user_id = null,
# but the different options are provided for reference.
Profile.create(bio: 'A l3t3 haker', user: dano)
dano.profile = Profile.create(bio: 'A l33t haker')
dano.create_profile(bio: 'Leet!')

# A few ways to add a post to user (one-to-many relationship)
dano.posts.create(content: 'Sample post')
Post.create(content: 'Another post', user: dano)
dano.posts.append(Post.create(content: 'A third post'))

dano.posts.destroy_all()

# Create the clans & relationships (many-to-many relationship)
dano.clans.create(name: 'Belgian ROFLs')
Clan.create(name: 'Hax0rs', users: [dano])
dano.clans.append(Clan.create(name: 'Lone Rangers'))

# Pulling the related objects
u = User.find_by_name('NanoDano')
puts ""
puts u.inspect
puts u.profile.inspect
puts "PostIDs: " + u.post_ids.inspect
puts u.posts.inspect
puts "ClanIDs: " + u.clan_ids.inspect
puts u.clans.inspect