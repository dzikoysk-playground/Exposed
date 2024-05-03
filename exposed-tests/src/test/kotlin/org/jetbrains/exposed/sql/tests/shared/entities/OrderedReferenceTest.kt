package org.jetbrains.exposed.sql.tests.shared.entities

import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.SortOrder.DESC
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.tests.DatabaseTestsBase
import org.jetbrains.exposed.sql.tests.TestDB
import org.jetbrains.exposed.sql.tests.shared.assertEquals
import org.jetbrains.exposed.sql.tests.shared.assertTrue
import org.junit.Test

class OrderedReferenceTest : DatabaseTestsBase() {
    object Users : IntIdTable()

    object UserRatings : IntIdTable() {
        val value = integer("value")
        val user = reference("user", Users)
    }

    class User(id: EntityID<Int>) : IntEntity(id) {
        companion object : IntEntityClass<User>(Users)

        val ratings by UserRating referrersOn UserRatings.user orderBy UserRatings.value
    }

    class UserRating(id: EntityID<Int>) : IntEntity(id) {
        companion object : IntEntityClass<UserRating>(UserRatings)

        var value by UserRatings.value
        var user by User referencedOn UserRatings.user
    }

    class UserDescOrdered(id: EntityID<Int>) : IntEntity(id) {
        companion object : IntEntityClass<UserDescOrdered>(Users)

        val ratings by UserRatingDescOrdered referrersOn UserRatings.user orderBy (UserRatings.value to DESC)
    }

    class UserRatingDescOrdered(id: EntityID<Int>) : IntEntity(id) {
        companion object : IntEntityClass<UserRatingDescOrdered>(UserRatings)

        var value by UserRatings.value
        var user by UserDescOrdered referencedOn UserRatings.user
    }

    class UserMultiColumnOrder(id: EntityID<Int>) : IntEntity(id) {
        companion object : IntEntityClass<UserMultiColumnOrder>(Users)

        val ratings by UserRatingMultiColumnOrder referrersOn UserRatings.user orderBy listOf(UserRatings.value to DESC, UserRatings.id to DESC)
    }

    class UserRatingMultiColumnOrder(id: EntityID<Int>) : IntEntity(id) {
        companion object : IntEntityClass<UserRatingMultiColumnOrder>(UserRatings)

        var value by UserRatings.value
        var user by UserMultiColumnOrder referencedOn UserRatings.user
    }

    @Test
    fun testDefaultOrder() {
        withOrderedReferenceTestTables {
            val user = User.all().first()

            unsortedRatingValues.sorted().zip(user.ratings).forEach { (value, rating) ->
                assertEquals(value, rating.value)
            }
        }
    }

    @Test
    fun testDescOrder() {
        withOrderedReferenceTestTables {
            val user = UserDescOrdered.all().first()

            unsortedRatingValues.sorted().reversed().zip(user.ratings).forEach { (value, rating) ->
                assertEquals(value, rating.value)
            }
        }
    }

    @Test
    fun testMultiColumnOrder() {
        withOrderedReferenceTestTables {
            val ratings = UserMultiColumnOrder.all().first().ratings.map { it }
            for (i in 1..<ratings.size) {
                val current = ratings[i]
                val prev = ratings[i - 1]
                assertTrue(current.value <= prev.value)
                if (current.value == prev.value) {
                    assertTrue(current.id <= prev.id)
                }
            }
        }
    }

    private val unsortedRatingValues = listOf(0, 3, 1, 2, 4, 4, 5, 4, 5, 6, 9, 8)

    private fun withOrderedReferenceTestTables(statement: Transaction.(TestDB) -> Unit) {
        withTables(Users, UserRatings) { db ->
            val userId = Users.insert { }.resultedValues?.firstOrNull()?.get(Users.id) ?: error("User was not created")
            unsortedRatingValues.forEach { value ->
                UserRatings.insert {
                    it[user] = userId
                    it[UserRatings.value] = value
                }
            }
            statement(db)
        }
    }
}
