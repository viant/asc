package asc_test



//REFACTORING ....
//
import (
	"fmt"
	"testing"
	"time"

	_ "github.com/aerospike/aerospike-client-go"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/asc"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"os"
	"sync/atomic"
	"log"
	"github.com/viant/assertly"
)

type User struct {
	Id             int             `column:"id"`
	Username       string          `column:"username"`
	Active         bool            `column:"active"`
	LastAccessTime *time.Time      `column:"last_time"`
	Salary         float64         `column:"salary"`
	Comments       string          `column:"comments"`
	Photo          []byte          `column:"photo"`
	Generation     uint64          `column:"scn"`
	CitiesLived    *[]string       `column:"city_lived"`
	CountryLived   []string        `column:"country_lived"`
	CitiesVisited  map[string]int  `column:"city_visited"`
	CountryVisited *map[string]int `column:"country_visit"`
}

func (this User) String() string {
	return fmt.Sprintf("Id: %v, Name: %v, Active:%v, Salary: %v Comments: %v, Last Access Time %v\n", this.Id, this.Username, this.Active, this.Salary, this.Comments, this.LastAccessTime)
}

var inited int32


func initDb(t *testing.T) bool {

	if !toolbox.FileExists(path.Join(os.Getenv("HOME"), ".secret/bq.json")) {
		return false
	}

	if atomic.LoadInt32(&inited) == 1 {
		return true
	}
	result := dsunit.InitFromURL(t, "test/init.yaml")
	atomic.StoreInt32(&inited, 1)
	return result

}



func GetManager(t *testing.T) dsc.Manager {

	config, err := dsc.NewConfigFromURL("test/config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}


func TestJSONUDF(t *testing.T) {
	if ! initDb(t) {
		return
	}
	dsunit.PrepareFor(t, "test", "test/", "ReadUDF")

	manager := GetManager(t)
	var record = map[string]interface{}{}

	success, err := manager.ReadSingle(&record, `SELECT 
id, 
username, 
city_visited.Warsaw AS warsaw_visit_count, 
JSON(city_visited) AS visted_json ,
ARRAY(city_visited) AS visited_array
FROM users WHERE id = ?`, []interface{}{1}, nil)
	assert.True(t, success)
	assert.Nil(t, err)
	var expected = `{
	"id": 1,
	"username": "Edi",
	"visited_array": [
		{"@indexBy@":"key"},
		{
			"key": "Las Vegas",
			"value": 1
		},
		{
			"key": "Warsaw",
			"value": 3
		}
	],
	"visted_json": "{\n\t\"Las Vegas\": 1,\n\t\"Warsaw\": 3\n}",
	"warsaw_visit_count": 3
}`
	assertly.AssertValues(t, expected, record)
}

func TestReadSingle(t *testing.T) {

	if ! initDb(t) {
		return
	}
	dsunit.PrepareFor(t, "test", "test/", "ReadSingle")

	manager := GetManager(t)
	singleUser := User{}
	success, err := manager.ReadSingle(&singleUser, "SELECT id, username, active, salary, comments,last_time, photo, city_lived, country_lived, city_visited, country_visit FROM users WHERE id = ?", []interface{}{1}, nil)
	if ! assert.Nil(t, err) {
		return
	}
	assert.Equal(t, true, success, "Should fetch a user")
	assert.Equal(t, "Edi", singleUser.Username)
	assert.Equal(t, true, singleUser.Active)
	assert.Equal(t, 123.32, singleUser.Salary)
	assert.Equal(t, "Test comment", singleUser.Comments)
	assert.Equal(t, "ABC", string(singleUser.Photo))
	assert.Equal(t, 2, len(*singleUser.CitiesLived))
	assert.Equal(t, "New York", (*singleUser.CitiesLived)[0])

	assert.Equal(t, 2, len(singleUser.CountryLived))
	assert.Equal(t, "USA", (singleUser.CountryLived)[0])

	assert.Equal(t, 2, len(singleUser.CitiesVisited))

	vistedCounter := singleUser.CitiesVisited["Las Vegas"]
	assert.Equal(t, 1, vistedCounter)

	assert.Equal(t, 3, (*singleUser.CountryVisited)["Poland"])

	_, _, err = manager.PersistSingle(&singleUser, "users", nil)
	assert.Nil(t, err)

}



func TestReadAll(t *testing.T) {

	if ! initDb(t) {
		return
	}
	dsunit.PrepareFor(t, "test", "test/", "ReadAll")
	manager := GetManager(t)
	assert.NotNil(t, manager)


	{
		var users = make([]User, 0)

		err := manager.ReadAll(&users, "SELECT id, username, active, salary, comments,last_time, photo, city_lived, country_lived, city_visited, country_visit  FROM users WHERE id IN (?,?)", []interface{}{1, 2}, nil)
		if err != nil {
			t.Error("Failed test: " + err.Error())
		}
		assert.Equal(t, 2, len(users))
		{
			user := users[0]
			assert.Equal(t, 1, user.Id)
			assert.Equal(t, "Edi", user.Username)
			assert.Equal(t, true, user.Active)
			assert.Equal(t, 123.32, user.Salary)
			assert.Equal(t, "Test comment", user.Comments)
			assert.Equal(t, "ABC", string(user.Photo))
		}
		{
			user := users[1]
			assert.Equal(t, 2, user.Id)
			assert.Equal(t, "Rudi", user.Username)
			assert.Equal(t, true, user.Active)
			assert.Equal(t, 234.0, user.Salary)
			assert.Equal(t, "def", user.Comments)
			assert.Equal(t, "bcd", string(user.Photo))
		}
	}

	{
		{
			var users = make([]User, 0)

			err := manager.ReadAll(&users, "SELECT id, username, active, salary, comments,last_time, photo, city_lived, country_lived, city_visited, country_visit  FROM users", nil, nil)
			if err != nil {
				t.Error("Failed test: " + err.Error())
			}

			var indexedUsers = make(map[int]User)
			for _, user := range users {
				indexedUsers[user.Id] = user
			}

			assert.Equal(t, 3, len(users))
			{
				user := indexedUsers[1]
				assert.Equal(t, 1, user.Id)
				assert.Equal(t, "Edi", user.Username)
				assert.Equal(t, true, user.Active)
				assert.Equal(t, 123.32, user.Salary)
				assert.Equal(t, "Test comment", user.Comments)
				assert.Equal(t, "ABC", string(user.Photo))
			}
			{
				user := indexedUsers[4]
				assert.Equal(t, 4, user.Id)
				assert.Equal(t, "Vudi", user.Username)
				assert.Equal(t, true, user.Active)
				assert.Equal(t, 143.0, user.Salary)
				assert.Equal(t, "xyz", user.Comments)
				assert.Equal(t, "cde", string(user.Photo))
			}
		}
	}
}

func TestPersistAll(t *testing.T) {
	if ! initDb(t) {
		return
	}
	dsunit.PrepareFor(t, "test", "test/", "PersistAll")
	manager := GetManager(t)
	assert.NotNil(t, manager)


	{
		var users = make([]User, 0)
		err := manager.ReadAll(&users, "SELECT * FROM users", nil, nil)
		assert.Nil(t, err)
		for i, user := range users {
			if user.Id == 4 {
				users[i].Username = "Hogi"
				users[i].CountryLived = []string{"USA", "France", "UK"}
				citiesLived := []string{"Los Angeles", "Paris", "London"}
				users[i].CitiesLived = &citiesLived
			}
		}
		citiesLived := []string{"Los Angeles", "London"}
		users = append(users, User{Id: 3, Username: "Gadi", Active: true, Salary: 123.4, Comments: "Abcdef",
			CountryLived: []string{"USA", "UK"},
			CitiesLived:  &citiesLived,
			CitiesVisited: map[string]int{
				"Warsaw": 1,
				"Berlin": 2,
				"France": 1,
			},
		})
		inserted, updated, err := manager.PersistAll(&users, "users", nil)
		assert.Nil(t, err)
		assert.Equal(t, 0, inserted)
		assert.Equal(t, 2, updated)

	}
	dsunit.ExpectFor(t, "test", dsunit.SnapshotDatasetCheckPolicy, "test/", "PersistAll")

}



func TestDelete(t *testing.T) {
	if ! initDb(t) {
		return
	}
	dsunit.PrepareFor(t, "test", "test/", "Delete")
	manager := GetManager(t)
	manager.Execute("DELETE FROM users WHERE id = ?", 4)

	dsunit.ExpectFor(t, "test", dsunit.FullTableDatasetCheckPolicy, "test/", "Delete")

}



func TestInvalidSql(t *testing.T) {
	manager := GetManager(t)
	_, err := manager.Execute("SET FROM users WHERE id = ?", 4)
	assert.NotNil(t, err)
}
