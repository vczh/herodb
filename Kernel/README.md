This is a draft.

# TYPE KEYWORDS
- bool
- `u?int(8|16|32|64)?`
- `float(32|64)?`
- `string`

# DEFINITION KEYWORDS
- `type`
- `struct`
- `object`
- `enum`
- `data`
- `index`

# QUERY KEYWORDS
- `query`
- `out`
- `order_by`
- `order_by_desc`
- `partition`
- `aggregate`

# TYPE

### PRIMITIVE-TYPE
- `bool`
- `u?int(8|16|32|64)?`
- `float(32|64)?`
- `string`
- `object`

### MISC
- `VALUE-TYPE:`: PRIMITIVE-TYPE, COMPLEX-TYPE
- `VALUE-TYPE?`: nullable

- operators:
	- `x^` : dereference, fail if null

### TYPE-DECLARATION:
- `type NAME = TYPE;`
- `enum Seasons {Spring, Summer, Autumn, Winter}`
- `struct Point {x : int, y : int}`
- `object NAME [: BASE-TYPE] STRUCT-TYPE`

### DATA-COLLECTION:
```
data AttendExams(
	s : Student,
	t : Teacher,
	e : Exam,
	score : int
) index {
	Hash(s);
	Hash(e);
	Unique(e, s);
	partition(e) {
		Ordered(score);
		Unique(t);
	}
}
```

### OBJECT-COLLECTION:
```
object Person {
	name : string,
	id : string
}
data People(default p : Person) index {
	Hash(name);
	Unique(data.id);
}
// data Xs(default x : X) means every instance X will  automatically appears in Xs
// every object should have exactly one default data collection

object Student : Person {
}
data Students(default s : Student) index {
	People(data);
}

object School {
	name : string,
}
data Schools(default s : School) index {
	Unique(name);
}

data AttendSchool (
	student : Student,
	school : School
) index {
	Require(Students(student), Schools(school));
	partition(student) {
		Unique(school);
	}
}
```

# QUERY

```
enum Gender {
	Male,
	Female
}

object Person {
	name : string,
	gender : Gender
}

data Relation(parent : Person, child : Person) index {
	partition(child) {
		Unique(Person);
	}
}
```

### Simple Query
```
query GrandParents(grandParent : Person, grandChild : Person)
{
	Relation(grandParent, parent);
	Relation(parent, grandSon);
}
```

### Output only argument
```
query Square(x : int, out x2 : int)
{
	x2 <- x * x;
}
// out keyword is required
```

### Cached Query
```
query GrandParents(grandParent : Person, grandChild : Person) index {
	Partition(grandParent);
}
{
	Relation(grandParent, parent);
	Relation(parent, grandSon);
}

// When submit a query, the index for caching is used to see if it is calculated
// If not, insert an index with the "calculating" status
// Adding an existing calculating index will cause an error (stop), which is not a failure (fail to pass a filter)
```

### order_by, order_by_desc
```
data Exams(student : string, score : int) index {
	Unique(student)
}

query Top10(out student : string, out score : int)
{
	Exams(student, score);
	order_by_desc(score)->index {
		index < 10;
	}
}
```

### partition
```
data Exams(student : string, score : int);

query Top3ScorePerStudent(student : string, out score : int, out index : int)
{
	Exams(student, score);
	partition(student) {
		order_by_desc(score)->index {
			index < 3;
		}
	}
}
```

### aggregation
```
data Exams(student : string, score : int);

query AverageTop3ScorePerStudent(student : string, out average : int) index {
	Unique(student) // should match the code
}
{
	Exams(student, score?); // bind result and create a new name: score
	partition(student) {
		order_by_desc(score)->index {
			index < 3
		}
		aggregate {
			average <- sum(score) / count(score);
		}
	}
}
```

# Expressions (cannot run backward)

# Transaction

# Database Updating
