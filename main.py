import pyodbc
from datetime import datetime


class YelpDatabaseApp:
    
    user_id = 'mab28'  # change the userId of the person's database(MARKING TA)

    

    def __init__(self):
        self.loggedInUserId = self.user_id
       
        self.connection = pyodbc.connect(
            'driver={ODBC Driver 18 for SQL Server};server=cypress.csil.sfu.ca;uid=s_mab28;pwd=Tn22yTm46baMgEa7;Encrypt=yes;TrustServerCertificate=yes')

    def login(self):
        users = [self.user_id]
        print("Yelp Database")
        while True:
            login = input("Please enter the userId : ")
            if login in users:
                print("%s logged in successfully." % login)
                self.display_menu()
                break
            else:
                print("User not found. Please try again.")

    def display_menu(self):
        print("Yelp Database")
        print("1. Search Business")
        print("2. Search Users")
        print("3. Make Friend")
        print("4. Review Business")
        print("5. Exit")
        choice = int(input("Enter your choice: "))
        self.handle_menu_choice(choice)

    def handle_menu_choice(self, choice):
        if choice == 1:
            self.search_business()
        elif choice == 2:
            self.search_user()
        elif choice == 3:
            self.make_friend()
        elif choice == 4:
            self.review_business()
        elif choice == 5:
            print("Exiting Yelp Database App. Goodbye!")
            self.connection.close()
            exit()
        else:
            print("Invalid Choice, try again.")

    class User:
        def __init__(self, user_id, name, review_count, yelping_since, useful, funny, cool, fans, average_stars):
            self.user_id = user_id
            self.name = name
            self.review_count = review_count
            self.yelping_since = yelping_since
            self.useful = useful
            self.funny = funny
            self.cool = cool
            self.fans = fans
            self.average_stars = average_stars

        def __str__(self):
            return f"User{{user_id='{self.user_id}', name='{self.name}', review_count={self.review_count}, " \
                   f"yelping_since={self.yelping_since}, useful={self.useful}, funny={self.funny}, " \
                   f"cool={self.cool}, fans={self.fans}, average_stars={self.average_stars}}}"

    class Business:
        def __init__(self, business_id, name, address, city, postal_code, stars, review_count):
            self.business_id = business_id
            self.name = name
            self.address = address
            self.city = city
            self.postal_code = postal_code
            self.stars = stars
            self.review_count = review_count

        def __str__(self):
            return f"Business{{business_id='{self.business_id}', name='{self.name}', address='{self.address}', " \
                   f"city='{self.city}', postal_code='{self.postal_code}', stars={self.stars}, " \
                   f"review_count={self.review_count}}}"

    def search_business(self):
        min_stars = int(input("Enter minimum number of stars: "))
        city = input("Enter city: ")
        business_name = input("Enter business name or part of the name: ")
        ordering_choice = int(input("Choose ordering (1. by name, 2. by city, 3. by stars): "))

        try:
            cursor = self.connection.cursor()
            query = "SELECT * FROM business WHERE stars >= ? AND city LIKE ? AND name LIKE ?"
            ordering = {1: " ORDER BY name", 2: " ORDER BY city", 3: " ORDER BY stars"}.get(ordering_choice, "")
            cursor.execute(query + ordering, min_stars, f"%{city}%", f"%{business_name}%")
            rows = cursor.fetchall()

            businesses = [self.Business(*row) for row in rows]

            if businesses:
                print("Search results:")
                for business in businesses:
                    print(business)
            else:
                print("No businesses found matching the criteria.")

        except pyodbc.Error as e:
            print("Error during business search. Please try again.")
            raise RuntimeError(e)

    def search_user(self):
        users = []
        user_name = input("Enter user name or part of the name: ")
        min_review_count = int(input("Enter minimum review count: "))
        min_average_stars = float(input("Enter minimum average stars: "))

        try:
            cursor = self.connection.cursor()
            query = "SELECT * FROM user_yelp WHERE name LIKE ? AND review_count >= ? AND average_stars >= ?"
            cursor.execute(query, f"%{user_name}%", min_review_count, min_average_stars)
            rows = cursor.fetchall()

            users = [self.User(*row) for row in rows]

            if users:
                print("Search results:")
                for user in users:
                    print(user)
            else:
                print("No users found in the database.")

        except pyodbc.Error as e:
            print("Error during user search. Please try again.")
            raise RuntimeError(e)

        return users

    def make_friend(self):
        users = self.search_user()

        if users:
            print("Select a user to make friends:")
            self.display_users(users)

            friend_id = input("Enter the user ID to make friends: ")

            if self.is_valid_user(friend_id, users):
                try:
                    cursor = self.connection.cursor()
                    query = "INSERT INTO friendship VALUES (?, ?)"
                    cursor.execute(query, self.loggedInUserId, friend_id)
                    self.connection.commit()
                    print("The friendship has been added successfully.")
                except pyodbc.Error as e:
                    print("Error during making friend. Please try again.")
                    raise RuntimeError(e)
            else:
                print("Invalid user ID. Please select a valid user.")
        else:
            print("No users found. Cannot create a friendship.")

    def is_valid_user(self, user_id, users):
        return any(user.user_id == user_id for user in users)

    def display_users(self, users):
        for user in users:
            print(user)

    def review_business(self):
        business_id = input("Please enter BusinessId to review: ")
        review_id = input("Please enter reviewId ")
        stars = int(input("Please enter the number of stars (1-5 any number): "))
        useful = int(input("Please enter the number of useful votes: "))
        funny = int(input("Please enter the number of funny votes: "))
        cool = int(input("Please enter the number of cool votes: "))

        try:
            cursor = self.connection.cursor()

            #"CREATE TRIGGER updateReviewsBusiness ON review AFTER INSERT AS BEGIN update business set review_count = (select count(*) from review where (select max(date) max_date from review r where r.user_id = review.user_id and business_id = inserted.business_id group by user_id) = review.date), stars = (select AVG(CAST(review.stars AS DECIMAL(2,1))) from review where (select max(date) max_date from review r where r.user_id = review.user_id and business_id = inserted.business_id group by user_id) = review.date) from business, inserted where business.business_id = inserted.business_id END;"
            

            query = "INSERT INTO review (review_id, user_id, business_id, stars, useful, funny, cool, date) " \
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(query, review_id, self.loggedInUserId, business_id, stars, useful, funny, cool, datetime.now())
            self.connection.commit()

            business_query = "CREATE TRIGGER updateReviewsBusiness ON review AFTER INSERT AS BEGIN update business set review_count = (select count(*) from review where (select max(date) max_date from review r where r.user_id = review.user_id and business_id = inserted.business_id group by user_id) = review.date), stars = (select AVG(CAST(review.stars AS DECIMAL(2,1))) from review where (select max(date) max_date from review r where r.user_id = review.user_id and business_id = inserted.business_id group by user_id) = review.date) from business, inserted where business.business_id = inserted.business_id END;"
            cursor.execute(business_query)
            self.connection.commit()

            print("Review submitted successfully.")

        except pyodbc.Error as e:
            print("Error during business review. Please try again.")
            raise RuntimeError(e)

    def run(self):
       
        self.login()
        while True:
            choice = self.display_menu()

            if choice == 1:
                self.search_business()
            elif choice == 2:
                self.search_user()
            elif choice == 3:
                self.make_friend()
            elif choice == 4:
                self.review_business()
            elif choice == 5:
                print("Exiting Yelp Database App. Goodbye!")
                break
            else:
                print("Invalid Choice, try again.")

        self.connection.close()

if __name__ == "__main__":
    app = YelpDatabaseApp()
    app.run()
