/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */
package asc_test

import (
	"testing"
	"time"
	"github.com/viant/dsc"
	"fmt"
	"github.com/stretchr/testify/assert"
	_ "github.com/aerospike/aerospike-client-go"
	_ "github.com/viant/asc"
	"github.com/viant/dsunit"
)



type User struct {
	Id              int `column:"id"`
	Username        string `column:"username"`
	Active          bool `column:"active"`
	LastAccessTime  *time.Time  `column:"last_time"`
	Salary          float64 `column:"salary"`
	Comments        string `column:"comments"`
	Photo           []byte `column:"photo"`
	Generation      uint64 `column:"scn"`
	CitiesLived     *[]string `column:"city_lived"`
	CountryLived     []string `column:"country_lived"`
	CitiesVisited map[string]int `column:"city_visited"`
	CountryVisited *map[string]int `column:"country_visit"`
}


func (this User) String() string {
	return fmt.Sprintf("Id: %v, Name: %v, Active:%v, Salary: %v Comments: %v, Last Access Time %v\n", this.Id, this.Username, this.Active, this.Salary, this.Comments, this.LastAccessTime)
}



func Manager(t *testing.T)  dsc.Manager {
	config := dsc.NewConfig("aerospike", "", "host:127.0.0.1,port:3000,namespace:test,generationColumnName:generation,dateLayout:2006-01-02 15:04:05.000")
	factory := dsc.NewManagerFactory()
	manager, _:=  factory.Create(config)
	return manager
}





func TestReadSingle(t *testing.T) {

	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.PrepareDatastoreFor(t, "test", "test://test/", "ReadSingle")

	manager := Manager(t)
	singleUser := User{}
	success, err:= manager.ReadSingle(&singleUser, "SELECT id, username, active, salary, comments,last_time, photo, city_lived, country_lived, city_visited, country_visit FROM users WHERE id = ?", []interface{}{1}, nil)
	assert.Nil(t, err)
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

	vistedCounter := singleUser.CitiesVisited["Las Vegas"];
	assert.Equal(t, 1, vistedCounter)

	assert.Equal(t, 3, (*singleUser.CountryVisited)["Poland"])


	_, _, err = manager.PersistSingle(&singleUser, "users", nil)
	assert.Nil(t, err)

}




func TestReadAll(t *testing.T) {

	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.PrepareDatastoreFor(t, "test", "test://test/", "ReadAll")
	manager := Manager(t)

	{
		var users = make([]User, 0)

		err := manager.ReadAll(&users, "SELECT id, username, active, salary, comments,last_access_time, photo, city_lived, country_lived, city_visited, country_visit  FROM users WHERE id IN (?,?)", []interface{}{1, 2}, nil)
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

			err := manager.ReadAll(&users, "SELECT id, username, active, salary, comments,last_access_time, photo, city_lived, country_lived, city_visited, country_visit  FROM users", nil, nil)
			if err != nil {
				t.Error("Failed test: " + err.Error())
			}


			var indexedUsers = make(map[int]User)
			for _, user:= range users {
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
				assert.Equal(t, 143.0	, user.Salary)
				assert.Equal(t, "xyz", user.Comments)
				assert.Equal(t, "cde", string(user.Photo))
			}
		}
	}
}



func TestPersistAll(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.PrepareDatastoreFor(t, "test", "test://test/", "PersistAll")
	manager := Manager(t)

	{
		var users = make([]User, 0)
		err := manager.ReadAll(&users, "SELECT * FROM users", nil, nil)
		assert.Nil(t, err)
		for i, user := range users {
			if user.Id == 4 {
				users[i].Username = "Hogi"
				users[i].CountryLived=[]string{"USA", "France", "UK"}
				citiesLived := []string{"Los Angeles", "Paris", "London"}
				users[i].CitiesLived = &citiesLived
			}
		}
		citiesLived := []string{"Los Angeles", "London"}
		users = append(users, User{Id:3, Username:"Gadi", Active:true, Salary:123.4, Comments:"Abcdef",
			CountryLived:[]string{"USA", "UK"},
			CitiesLived: &citiesLived,
			CitiesVisited:map[string]int{
				"Warsaw":1,
				"Berlin":2,
				"France":1,
			},
		})
		inserted, updated, err := manager.PersistAll(&users, "users", nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, inserted)
		assert.Equal(t, 3, updated)

	}
	dsunit.ExpectDatasetFor(t, "test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "PersistAll")
	dsunit.ExpectDatasetFor(t, "test", dsunit.FullTableDatasetCheckPolicy, "test://test/", "PersistAll")

}