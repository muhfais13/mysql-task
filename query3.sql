SELECT 
    st.student_id AS student,
    st.student_name AS name,
    s.subject_name AS subject,
    COALESCE(MAX(CASE WHEN er.exam_event = 'Exam 1' THEN er.exam_score END), '0') AS exam_1,
    COALESCE(MAX(CASE WHEN er.exam_event = 'Exam 2' THEN er.exam_score END), '0') AS exam_2,
    ROUND(AVG(er.exam_score), 2) AS average_exam_score,
    CASE
        WHEN AVG(er.exam_score) >= 90 THEN 'A'
        WHEN AVG(er.exam_score) < 90 AND AVG(er.exam_score) >= 80 THEN 'B'
        WHEN AVG(er.exam_score) < 80 AND AVG(er.exam_score) >= 70 THEN 'C'
        WHEN AVG(er.exam_score) < 70 AND AVG(er.exam_score) >= 50 THEN 'D'
        ELSE 'F'
    END AS grade
FROM 
    student st
JOIN 
    exam_result er ON st.student_id = er.student_id
JOIN 
    subject s ON er.subject_id = s.subject_id
GROUP BY 
    st.student_id, st.student_name, s.subject_name
ORDER BY 
    st.student_id, s.subject_name;