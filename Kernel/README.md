This is a draft.

# HIGH-LEVEL FEATURES
- Memory DB
- Functional Programming
- External resource accessing
  - Auto Backup to file group ring (maintain diff bitmaps for all file groups)
  - Generate the whole data base to C++
- Data Package
	- Package includes schema, data, procedures
	- Strong typed package non-circle dependency (package can access depended packages)
	- Multiple instances of one data package
	- Objects are allocated and stored inside one package, an object cannot be stored in a package that did not create it
	- Object can be deleted, but the handle is never reused
	- A data package can be deleted as a whole operation, which require all other packages that depend on it are deleted

# TYPE

### PRIMITIVE-TYPE
- `bool`
- `u?int(8|16|32|64)?`
- `float(32|64)?`
- `string`

### VALUE-TYPE
- PRIMITIVE-TYPE
- `enum {Spring, Summer, Autumn, Winter}`
- `struct {x : int, y : int}`

### COMPLEX-TYPE
- `A | B`: union type
- `[ TYPE ]`: enumerable type
- `VALUE-TYPE?`: nullable type

### CLASS-TYPE
- `class[:BASE-CLASS-TYPE]{x : int, y : int}`: reference counted class type, not nullable
- `CLASS-TYPE?`: nullable class type

### DATA-COLLECTION:

- Declaration:
```
type AttendExam = {
	s: Student;
	t: Teacher;
	e: Exam;
	score: int;
}

data AttendExams(AttendExam);
```
- Or Equivalently
```
data AttendExams(s: Student, t: Teacher, e: Exam, score: int);
```

- Index:
```
index AttendExams {
	Hash(s);
	Hash(e);
	Unique(e, s);
	partition(e) {
		Ordered(score);
		Unique(t);
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

data Father(father: Person, child: Person);
index Father{Unique(child);}

data Mother(father: Person, child: Person);
index Mother {
	Unique(child);
}

data Relation(parent: Person, child: Person);
index Relation {
	partition(child) {
		Unique(Person);
	}
}
```

### Simple Query
```
query Parents(parent : Person, child : Child)
	:- Father(parent, child);
	:- Mother(parent, child);

query GrandParents(grandParent : Person, grandChild : Person)
:- {
	query(parent)
		:- Father(parent, child);
		:- Mother(parent, child);
	Parents(grandParent, parent);
}
```

### Output only argument
```
query Square(x : int, out x2 : int)
	:- x2 <- x * x;
// <- define the execution direction, it cannot run backward from x2 to x
// out keyword is required
```

### Cached Query
```
query GrandParents(grandParent : Person, grandChild : Person)
:- {
	Parents(grandParent, parent);
	Parents(parent, grandChild);
}

index GrandParents {
	Hash(grandParent)
}

// When submit a query, the index for caching is used to see if it is calculated
// If not, insert an index with the "calculating" status
// Adding an existing calculating index will cause an error (stop), which is not a failure (fail to pass a filter)
```

### order_by, order_by_desc
```
struct Exams{student : string; score : int;}
index Exams {
	Unique(student);
}

query Top10(out student : string, out score : int)
:- {
	Exams(student, score);
	order_by_desc(score)->order?
		:- order < 10;
}
```

### partition
```
struct Exams{student : string; score : int;}
index Exams {
}

query Top3ScorePerStudent(student : string, out score : int, out order : int)
:- {
	Exams(student, score);
	partition(student)
		:- order_by_desc(score)->order?
			:- order < 3;
}
```

### aggregation
```
struct Exams{student : string; score : int;}
index Exams {
}

// the order is not important
index AverageTop3ScorePerStudent {
	Unique(student);
}
query AverageTop3ScorePerStudent(student : string, out average : int)
:- {
	Exams(student, score?); // bind result and create a new name: score
	partition(student)
	:- {
		order_by_desc(score)->order?
			:- order < 3 ;
		aggregate
			:- average <- average(score);
		// aggregated value becomes inaccessable
		// only partitioned keys and aggregation results are accessable
	}
}

```

# UPDATE

# EXPRESSION (cannot run backward)

# TRANSACTION and PROCEDURE

# UPDATE (schema)

# DATA PACKAGE
